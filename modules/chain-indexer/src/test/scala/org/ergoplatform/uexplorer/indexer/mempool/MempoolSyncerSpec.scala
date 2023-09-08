package org.ergoplatform.uexplorer.indexer.mempool

import org.ergoplatform.uexplorer.TxId
import org.ergoplatform.uexplorer.http.{Codecs, TestSupport}
import org.ergoplatform.uexplorer.node.ApiTransaction
import zio.test.*

import scala.collection.immutable.{ArraySeq, ListMap}

object MempoolSyncerSpec extends ZIOSpecDefault with TestSupport with Codecs:

  def txs(ids: Seq[Int]) =
    ListMap(
      ids.map(i => ApiTransaction(TxId(i.toString), ArraySeq.empty, List.empty, ArraySeq.empty, Option.empty)).map(tx => tx.id -> tx): _*
    )

  def spec =
    suite("MempoolSyncerSpec")(
      test("MemPoolState") {
        val firstTwoTxState                            = MemPoolState.empty.applyStateChange(txs(1 to 2))
        val (firstTwoTxsChanges, firstTwoTxsState)     = firstTwoTxState
        val anotherTwoTxState                          = firstTwoTxsState.applyStateChange(txs(3 to 4))
        val (anotherTwoTxsChanges, anotherTwoTxsState) = anotherTwoTxState
        assertTrue(
          firstTwoTxsState.underlyingTxs.size == 2,
          firstTwoTxsChanges.stateTransitionByTx.size == 2,
          firstTwoTxsChanges.stateTransitionByTx.head._2.size == 0,
          firstTwoTxsChanges.stateTransitionByTx.last._2.size == 1,
          anotherTwoTxsState.underlyingTxs.size == 2,
          anotherTwoTxsState.underlyingTxs.map(_._1) == List(TxId("3"), TxId("4")),
          anotherTwoTxsChanges.stateTransitionByTx.size == 2,
          anotherTwoTxsChanges.stateTransitionByTx.map(_._1.id) == List(TxId("3"), TxId("4")),
          anotherTwoTxsChanges.stateTransitionByTx.head._2.size == 0,
          anotherTwoTxsChanges.stateTransitionByTx.last._2.size == 1
        )
      }
    )
