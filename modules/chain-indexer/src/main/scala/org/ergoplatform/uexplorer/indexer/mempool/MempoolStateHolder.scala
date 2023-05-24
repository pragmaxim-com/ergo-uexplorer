package org.ergoplatform.uexplorer.indexer.mempool

import akka.Done
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.chain.ChainState
import org.ergoplatform.uexplorer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.mempool.MempoolStateHolder.*
import org.ergoplatform.uexplorer.utxo.UtxoState
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.{Address, BoxId, TxId}

import scala.collection.immutable.{ArraySeq, ListMap}
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import org.ergoplatform.uexplorer.Tx

object MempoolStateHolder extends LazyLogging {

  def behavior(mempoolState: MempoolState): Behavior[MempoolStateHolderRequest] =
    Behaviors.setup[MempoolStateHolderRequest] { _ =>
      Behaviors.receiveMessage[MempoolStateHolderRequest] { case UpdateTxs(allTxs, replyTo) =>
        val (newState, stateChange) = mempoolState.applyStateChange(allTxs)
        replyTo ! stateChange
        behavior(newState)
      }
    }

  implicit private val timeout: Timeout = 3.seconds

  sealed trait MempoolStateHolderRequest
  sealed trait MempoolStateHolderResponse

  case class MempoolStateChanges(stateTransitionByTx: List[(ApiTransaction, ListMap[TxId, ApiTransaction])])
    extends MempoolStateHolderResponse {

    def utxoStateTransitionByTx(utxoState: UtxoState): Iterator[(ApiTransaction, UtxoState)] =
      stateTransitionByTx.iterator.map { case (newTx, poolTxs) =>
        val boxesByTxBuilder =
          poolTxs.values.foldLeft(
            Map.newBuilder[Tx, (ArraySeq[(BoxId, Address, Long)], ArraySeq[(BoxId, Address, Long)])]
          ) { case (acc, tx) =>
            val inputSet = tx.inputs.toSet.map(_.boxId)
            val inputsWithAddrValue =
              ArraySeq.from(
                utxoState.inputsByHeightBuffer.valuesIterator.flatMap { boxes =>
                  val shared = boxes.keySet.intersect(inputSet)
                  shared.map { boxId =>
                    val (addr, value) = boxes(boxId)
                    (boxId, addr, value)
                  }
                }
              )
            acc.addOne(Tx(tx.id, 0, 0, 0) -> (inputsWithAddrValue, tx.outputs.map(o => (o.boxId, o.address, o.value))))
          }
        newTx -> utxoState.mergeBlockBoxes(0, boxesByTxBuilder.result().iterator)
      }
  }

  case class UpdateTxs(allTxs: ListMap[TxId, ApiTransaction], replyTo: ActorRef[MempoolStateChanges])
    extends MempoolStateHolderRequest

  import akka.actor.typed.scaladsl.AskPattern.*

  case class MempoolState(underlyingTxs: ListMap[TxId, ApiTransaction]) {

    def applyStateChange(allTxs: ListMap[TxId, ApiTransaction]): (MempoolState, MempoolStateChanges) = {
      val newTxIds = allTxs.keySet.diff(underlyingTxs.keySet)
      val newTxs   = allTxs.filter(t => newTxIds.contains(t._1))
      val newState = MempoolState(underlyingTxs ++ newTxs)
      val stateChanges =
        newTxs.foldLeft(Vector.empty[(ApiTransaction, ListMap[TxId, ApiTransaction])]) {
          case (changes, newTx) if changes.isEmpty =>
            changes :+ (newTx._2, underlyingTxs)
          case (changes, newTx) =>
            val newUnderlying = changes.last._2.updated(changes.last._1.id, changes.last._1)
            changes :+ (newTx._2, newUnderlying)
        }
      newState -> MempoolStateChanges(stateChanges.toList)
    }
  }

  object MempoolState {
    def empty: MempoolState = MempoolState(ListMap.empty)
  }

  def updateTransactions(
    txs: ListMap[TxId, ApiTransaction]
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[UpdateTxs]): Future[MempoolStateChanges] =
    ref.ask(ref => UpdateTxs(txs, ref))

}
