package org.ergoplatform.uexplorer.indexer.mempool

import akka.Done
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.TxId
import org.ergoplatform.uexplorer.indexer.mempool.MempoolSyncer.*
import org.ergoplatform.uexplorer.node.ApiTransaction

import concurrent.duration.DurationInt
import scala.concurrent.Future

object MempoolSyncer extends LazyLogging {

  def behavior(mempoolState: MempoolState): Behavior[MempoolSyncerRequest] =
    Behaviors.setup[MempoolSyncerRequest] { _ =>
      Behaviors.receiveMessage[MempoolSyncerRequest] {
        case UpdateTxs(txs, replyTo) =>
          val newTxs = mempoolState.getNewTxs(txs)
          replyTo ! newTxs
          behavior(mempoolState.appendNewTxs(newTxs.txs))
        case GetMempoolState(replyTo) =>
          replyTo ! mempoolState
          Behaviors.same
      }
    }

  implicit private val timeout: Timeout = 3.seconds

  sealed trait MempoolSyncerRequest
  sealed trait MempoolSyncerResponse

  case class NewTransactions(txs: Map[TxId, ApiTransaction]) extends MempoolSyncerResponse
  case class UpdateTxs(txs: Map[TxId, ApiTransaction], replyTo: ActorRef[NewTransactions]) extends MempoolSyncerRequest
  case class GetMempoolState(replyTo: ActorRef[MempoolState]) extends MempoolSyncerRequest

  import akka.actor.typed.scaladsl.AskPattern._

  case class MempoolState(underlyingTxs: Map[TxId, ApiTransaction]) {

    def getNewTxs(newTxs: Map[TxId, ApiTransaction]): NewTransactions = NewTransactions(
      newTxs.keySet.diff(underlyingTxs.keySet).foldLeft(Map.empty[TxId, ApiTransaction]) { case (acc, txId) =>
        acc.updated(txId, newTxs(txId))
      }
    )

    def appendNewTxs(newTxs: Map[TxId, ApiTransaction]): MempoolState =
      MempoolState(underlyingTxs ++ newTxs)
  }

  def getMempoolState(implicit s: ActorSystem[Nothing], ref: ActorRef[GetMempoolState]): Future[MempoolState] =
    ref.ask(ref => GetMempoolState(ref))

  def updateTransactions(
    txs: Map[TxId, ApiTransaction]
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[UpdateTxs]): Future[NewTransactions] =
    ref.ask(ref => UpdateTxs(txs, ref))

}
