package org.ergoplatform.uexplorer.backend

import org.ergoplatform.uexplorer.BlockId
import org.ergoplatform.uexplorer.Const.Protocol
import org.ergoplatform.uexplorer.backend.blocks.BlockRepo
import org.ergoplatform.uexplorer.backend.boxes.BoxRepo
import org.ergoplatform.uexplorer.db.{Block, LinkedBlock}
import zio.*

import scala.collection.mutable

case class InmemoryRepo(blockRepo: BlockRepo, boxRepo: BoxRepo) extends Repo:
  override def isEmpty: Task[Boolean] =
    for
      blockRepoEmpty <- blockRepo.isEmpty
      boxRepoEmpty   <- boxRepo.isEmpty
    yield blockRepoEmpty && boxRepoEmpty

  override def removeBlocks(blockIds: Set[BlockId]): Task[Unit] = blockRepo.delete(blockIds).unit

  override def writeBlock(b: LinkedBlock)(preTx: Task[Any], postTx: Task[Any]): Task[BlockId] = {
    val ergoTrees   = b.outputRecords.byErgoTree.keys
    val ergoTreeT8s = b.outputRecords.byErgoTreeT8.keys
    val utxos       = b.outputRecords.byErgoTree.values.flatten
    for
      _       <- preTx
      blockId <- blockRepo.insert(b.block)
      _       <- boxRepo.insertUtxos(ergoTrees, ergoTreeT8s, utxos)
      _       <- boxRepo.deleteUtxos(b.b.transactions.transactions.flatMap(_.inputs.map(_.boxId)))
      _       <- postTx
    yield blockId
  }

object InmemoryRepo {
  def layer: ZLayer[BlockRepo with BoxRepo, Nothing, InmemoryRepo] =
    ZLayer.fromFunction(InmemoryRepo.apply _)

}
