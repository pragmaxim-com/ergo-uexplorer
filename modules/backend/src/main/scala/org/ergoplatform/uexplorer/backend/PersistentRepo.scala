package org.ergoplatform.uexplorer.backend

import io.getquill.*
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.Const.Protocol.{Emission, Foundation}
import org.ergoplatform.uexplorer.backend.blocks.BlockRepo
import org.ergoplatform.uexplorer.backend.boxes.*
import org.ergoplatform.uexplorer.db.*
import zio.*

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

  override def removeBlocks(blockIds: List[BlockId]): Task[Unit] = blockRepo.delete(blockIds).unit

  override def writeBlock(b: LinkedBlock): Task[BlockId] = {
    val inputIds: Seq[BoxId] =
      b.b.transactions.transactions
        .flatMap(_.inputs.collect { case i if i.boxId != Emission.inputBox && i.boxId != Foundation.inputBox => i.boxId })
    writeBlock(b, inputIds)
  }

  override def writeBlock(b: LinkedBlock, inputIds: Seq[BoxId]): Task[BlockId] = {
    val outputs     = b.outputRecords
    def ergoTrees   = outputs.byErgoTree.keys
    def ergoTreeT8s = outputs.byErgoTreeT8.keys
    def utxos       = outputs.byErgoTree.values.flatten
    def assets      = outputs.utxosByTokenId.keySet.map(tokenId => Asset(tokenId, b.block.blockId))
    def assetsToBox =
      outputs.tokensByUtxo.flatMap { case (box, amountByToken) =>
        amountByToken.map { case (token, amount) => Asset2Box(token, box, amount) }
      }

    ctx
      .transaction(
        blockRepo.insert(b.block) <* boxRepo.insertUtxos(ergoTrees, ergoTreeT8s, assetsToBox, assets, utxos) <* boxRepo.deleteUtxos(inputIds)
      )
      .as(b.block.blockId)
      .provide(dsLayer)
  }

  override def getLastBlock: Task[Option[Block]] =
    blockRepo.getLastBlocks(1).map(_.lastOption)

object PersistentRepo:
  def layer: ZLayer[DataSource with BlockRepo with BoxRepo, Nothing, PersistentRepo] =
    ZLayer.fromFunction(PersistentRepo.apply _)
