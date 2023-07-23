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

  override def isEmpty: Task[Boolean] =
    for {
      blockEmpty <- blockRepo.isEmpty
      boxEmpty   <- boxRepo.isEmpty
    } yield blockEmpty && boxEmpty

  override def removeBlocks(blockIds: Set[BlockId]): Task[Unit] = blockRepo.delete(blockIds).unit

  override def writeBlock(b: LinkedBlock)(preTx: Task[Any], postTx: Task[Any]): Task[BlockId] = {
    val outputs = b.outputRecords
    val inputs  = b.b.transactions.transactions.flatMap(_.inputs.map(_.boxId))
    persistBlockInTx(b.block, outputs, inputs, preTx, postTx)
  }

  private def persistBlockInTx(
    block: Block,
    outputs: OutputRecords,
    inputIds: Iterable[BoxId],
    preTx: Task[Any],
    postTx: Task[Any]
  ): Task[BlockId] = {
    val ergoTrees   = outputs.byErgoTree.keys
    val ergoTreeT8s = outputs.byErgoTreeT8.keys
    val utxos       = outputs.byErgoTree.values.flatten
    ctx
      .transaction {
        for
          _       <- preTx
          blockId <- blockRepo.insert(block)
          _       <- boxRepo.insertUtxos(ergoTrees, ergoTreeT8s, utxos)
          _       <- boxRepo.deleteUtxos(inputIds)
          _       <- postTx
        yield blockId
      }
      .as(block.blockId)
      .provide(dsLayer)
  }

object PersistentRepo:
  def layer: ZLayer[DataSource with BlockRepo with BoxRepo, Nothing, PersistentRepo] =
    ZLayer.fromFunction(PersistentRepo.apply _)
