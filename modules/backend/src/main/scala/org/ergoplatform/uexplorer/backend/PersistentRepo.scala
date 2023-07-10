package org.ergoplatform.uexplorer.backend

import io.getquill.*
import io.getquill.context.ZioJdbc.DataSourceLayer
import io.getquill.jdbczio.Quill
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.backend.Codecs
import org.ergoplatform.uexplorer.backend.blocks.{BlockRepo, PersistentBlockRepo}
import org.ergoplatform.uexplorer.backend.boxes.{BoxRepo, PersistentBoxRepo}
import org.ergoplatform.uexplorer.db.*
import zio.*

import java.util.UUID
import javax.sql.DataSource

case class PersistentRepo(ds: DataSource, blockRepo: BlockRepo, boxRepo: BoxRepo) extends Repo with Codecs:
  val ctx = new H2ZioJdbcContext(Literal)
  import ctx.*

  private val dsLayer = ZLayer.succeed(ds)

  override def persistBlockInTx(
    block: Block,
    outputs: OutputRecords,
    inputs: Iterable[BoxId]
  ): Task[BlockId] = {
    val ergoTrees   = outputs.byErgoTree.keys
    val ergoTreeT8s = outputs.byErgoTreeT8.keys
    val utxos       = outputs.byErgoTree.values.flatten
    ctx
      .transaction {
        for
          blockId <- blockRepo.insert(block)
          _       <- boxRepo.insertUtxos(ergoTrees, ergoTreeT8s, utxos)
          _       <- boxRepo.deleteUtxos(inputs)
        yield blockId
      }
      .as(block.blockId)
      .provide(dsLayer)
  }

object PersistentRepo:
  def layer: ZLayer[DataSource with BlockRepo with BoxRepo, Nothing, PersistentRepo] =
    ZLayer.fromFunction(PersistentRepo.apply _)
