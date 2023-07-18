package org.ergoplatform.uexplorer.indexer.mempool

import org.ergoplatform.uexplorer.ReadableStorage
import org.ergoplatform.uexplorer.http.BlockHttpClient

import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.http.BlockHttpClient
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.{BoxId, ErgoTreeHex, TxId}

import scala.collection.immutable.{ArraySeq, ListMap}
import scala.concurrent.duration.DurationInt
import zio.*

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
    val newState = MemPoolState(underlyingTxs ++ newTxs)
    val stateChanges =
      newTxs.foldLeft(Vector.empty[(ApiTransaction, ListMap[TxId, ApiTransaction])]) {
        case (changes, newTx) if changes.isEmpty =>
          changes :+ (newTx._2, underlyingTxs)
        case (changes, newTx) =>
          val newUnderlying = changes.last._2.updated(changes.last._1.id, changes.last._1)
          changes :+ (newTx._2, newUnderlying)
      }
    MemPoolStateChanges(stateChanges.toList) -> newState
  }
}

object MemPoolState {
  def empty: MemPoolState = MemPoolState(ListMap.empty)
}

case class MempoolSyncer(blockHttpClient: BlockHttpClient, memPool: MemPool) {

  def syncMempool(
    storage: ReadableStorage
  ): Task[MemPoolStateChanges] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    blockHttpClient.getBestBlockHeight.flatMap { bestBlockHeight =>
      if (storage.getLastBlocks.exists(_._2.height >= bestBlockHeight)) {
        for {
          txs          <- blockHttpClient.getUnconfirmedTxs
          stateChanges <- memPool.updateTxs(txs)
        } yield stateChanges
      } else {
        ZIO.succeed(MemPoolStateChanges(List.empty))
      }
    }
  }

}

object MempoolSyncer {
  def layer: ZLayer[BlockHttpClient with MemPool, Nothing, MempoolSyncer] =
    ZLayer.fromFunction(MempoolSyncer.apply _)
}
