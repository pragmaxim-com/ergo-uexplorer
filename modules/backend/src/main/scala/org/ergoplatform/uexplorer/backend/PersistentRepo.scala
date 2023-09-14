package org.ergoplatform.uexplorer.backend

import io.getquill.*
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.Const.Protocol.{Emission, Foundation}
import org.ergoplatform.uexplorer.backend.blocks.BlockRepo
import org.ergoplatform.uexplorer.backend.boxes.*
import org.ergoplatform.uexplorer.db.*
import zio.*

import javax.sql.DataSource

case class PersistentRepo(ds: DataSource, blockRepo: BlockRepo, boxRepo: BoxRepo, storage: WritableStorage) extends Repo with Codecs:
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
      outputs.tokensByUtxo.flatMap { case (boxId, tokenById) =>
        tokenById.map { case (tokenId, Token(index, amount, name, description, t, decimals)) =>
          Asset2Box(tokenId, boxId, index, amount, name, description, t, decimals)
        }
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

  override def persistBlock(b: LinkedBlock): Task[BestBlockInserted] = {
    val inputIds: Seq[BoxId] =
      b.b.transactions.transactions
        .flatMap(_.inputs.collect { case i if i.boxId != Emission.inputBox && i.boxId != Foundation.inputBox => i.boxId })

    def storageOps = List(
      storage.persistErgoTreeByUtxo(b.outputRecords.byErgoTree) *> storage.removeInputBoxesByErgoTree(inputIds),
      storage.persistErgoTreeT8ByUtxo(b.outputRecords.byErgoTreeT8) *> storage.removeInputBoxesByErgoTreeT8(inputIds),
      storage.persistUtxosByTokenId(b.outputRecords.utxosByTokenId) *> storage.persistTokensByUtxo(b.outputRecords.tokensByUtxo) *> storage
        .removeInputBoxesByTokenId(inputIds),
      storage.insertNewBlock(b.b.header.id, b.block, storage.getCurrentRevision)
    )

    for _ <- (ZIO.collectAllParDiscard(storageOps) *> storage.commit()) <&> writeBlock(b, inputIds)
    yield BestBlockInserted(b, None)
  }

object PersistentRepo:
  def layer: ZLayer[DataSource with BlockRepo with BoxRepo with WritableStorage, Nothing, PersistentRepo] =
    ZLayer.fromFunction(PersistentRepo.apply _)
