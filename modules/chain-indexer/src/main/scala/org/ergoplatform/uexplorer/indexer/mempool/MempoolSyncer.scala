package org.ergoplatform.uexplorer.indexer.mempool

import akka.Done
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.{Address, BoxId, TxId}
import org.ergoplatform.uexplorer.indexer.mempool.MempoolSyncer.*
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState
import org.ergoplatform.uexplorer.node.ApiTransaction

import concurrent.duration.DurationInt
import scala.collection.immutable.{ArraySeq, ListMap}
import scala.concurrent.Future

object MempoolSyncer extends LazyLogging {

  def behavior(mempoolState: MempoolState): Behavior[MempoolSyncerRequest] =
    Behaviors.setup[MempoolSyncerRequest] { _ =>
      Behaviors.receiveMessage[MempoolSyncerRequest] { case UpdateTxs(allTxs, replyTo) =>
        val (newState, stateChange) = mempoolState.applyStateChange(allTxs)
        replyTo ! stateChange
        behavior(newState)
      }
    }

  implicit private val timeout: Timeout = 3.seconds

  sealed trait MempoolSyncerRequest
  sealed trait MempoolSyncerResponse

  case class MempoolStateChanges(stateTransitionByTx: List[(ApiTransaction, ListMap[TxId, ApiTransaction])])
    extends MempoolSyncerResponse {

    def utxoStateTransitionByTx(utxoState: UtxoState): Iterator[(ApiTransaction, UtxoState)] =
      stateTransitionByTx.iterator.map { case (newTx, poolTxs) =>
        val (inputs, outputs) =
          poolTxs.values.foldLeft((ArraySeq.newBuilder[BoxId], ArraySeq.newBuilder[(BoxId, Address, Long)])) {
            case ((iAcc, oAcc), tx) =>
              iAcc.addAll(tx.inputs.map(_.boxId)) -> oAcc.addAll(tx.outputs.map(o => (o.boxId, o.address, o.value)))
          }
        newTx -> utxoState.mergeBoxes(List((inputs.result(), outputs.result())).iterator)
      }
  }

  case class UpdateTxs(allTxs: ListMap[TxId, ApiTransaction], replyTo: ActorRef[MempoolStateChanges])
    extends MempoolSyncerRequest

  import akka.actor.typed.scaladsl.AskPattern._

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

  def updateTransactions(
    txs: ListMap[TxId, ApiTransaction]
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[UpdateTxs]): Future[MempoolStateChanges] =
    ref.ask(ref => UpdateTxs(txs, ref))

}
