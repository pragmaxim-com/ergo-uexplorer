package org.ergoplatform.uexplorer.indexer.mempool

import org.ergoplatform.uexplorer.{ReadableStorage, TxId}
import org.ergoplatform.uexplorer.http.BlockHttpClient
import org.ergoplatform.uexplorer.node.ApiTransaction
import zio.*

import scala.collection.immutable.ListMap

case class MemPool(ref: Ref[MemPoolState]) {
  def updateTxs(allTxs: ListMap[TxId, ApiTransaction]): UIO[MemPoolStateChanges] =
    ref.modify(s => s.applyStateChange(allTxs))
  def getTxs: UIO[MemPoolState] = ref.get
}

object MemPool {
  def layer: ZLayer[Any, Nothing, MemPool] = ZLayer.fromZIO(Ref.make(MemPoolState.empty).map(MemPool(_)))
}

case class MemPoolStateChanges(stateTransitionByTx: List[(ApiTransaction, ListMap[TxId, ApiTransaction])])
case class MemPoolState(underlyingTxs: ListMap[TxId, ApiTransaction]) {

  def applyStateChange(allTxs: ListMap[TxId, ApiTransaction]): (MemPoolStateChanges, MemPoolState) = {
    val newTxIds = allTxs.keySet.diff(underlyingTxs.keySet)
    val newTxs   = allTxs.filter(t => newTxIds.contains(t._1))
    val newState = MemPoolState(allTxs)
    val stateChanges =
      newTxs.foldLeft(Vector.empty[(ApiTransaction, ListMap[TxId, ApiTransaction])]) { case (changes, newTx) =>
        val newUnderlying = changes.lastOption.fold(ListMap.empty)(_._2.updated(changes.last._1.id, changes.last._1))
        changes :+ (newTx._2, newUnderlying)
      }
    MemPoolStateChanges(stateChanges.toList) -> newState
  }
}

object MemPoolState {
  def empty: MemPoolState = MemPoolState(ListMap.empty)
}

case class MempoolSyncer(blockHttpClient: BlockHttpClient, storage: ReadableStorage, memPool: MemPool) {

  def syncMempool: Task[MemPoolStateChanges] =
    blockHttpClient.getBestBlockHeight.flatMap { bestBlockHeight =>
      if (storage.getLastBlocks.exists(_._2.height >= bestBlockHeight)) {
        for {
          txs          <- blockHttpClient.getUnconfirmedTxs
          stateChanges <- memPool.updateTxs(txs)
        } yield stateChanges
      } else {
        ZIO.succeed(MemPoolStateChanges(List.empty)) // we sync mempool only when DB is synced with network
      }
    }

}

object MempoolSyncer {
  def layer: ZLayer[BlockHttpClient with MemPool with ReadableStorage, Nothing, MempoolSyncer] =
    ZLayer.fromFunction(MempoolSyncer.apply _)
}
