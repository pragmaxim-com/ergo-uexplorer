package org.ergoplatform.uexplorer.indexer.progress

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.pattern.StatusReply
import akka.util.Timeout
import org.ergoplatform.uexplorer.db.Block
import akka.actor.typed.scaladsl.AskPattern.*
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.progress.ProgressState.CachedBlock

import scala.collection.immutable.TreeMap
import scala.concurrent.duration.*
import scala.concurrent.Future

object UtxoHolder extends LazyLogging {
  implicit private val timeout: Timeout = 3.seconds

  def initialBehavior: Behavior[HolderRequest] =
    Behaviors.setup[HolderRequest] { _ =>
      initialized(UtxoState(TreeMap.empty, Map.empty, Map.empty)) // TODO fix to persistent maps
    }

  def initialized(state: UtxoState): Behaviors.Receive[HolderRequest] =
    Behaviors.receiveMessage[HolderRequest] {
      case AddBestBlock(bestBlock, replyTo) =>
        val newState =
          state.addBestBlock(
            bestBlock.header.height,
            bestBlock.inputs.map(_.boxId),
            bestBlock.outputs.map(o => o.boxId -> o.address)
          )
        replyTo ! StatusReply.success(newState)
        initialized(newState)
      case AddForkBlock(newFork, supersededFork, replyTo) =>
        val newForkByHeight =
          newFork.map(b => b.header.height -> (b.inputs.map(_.boxId), b.outputs.map(o => o.boxId -> o.address))).toMap
        val newState = state.addFork(newForkByHeight, supersededFork.map(_.height))
        replyTo ! StatusReply.success(newState)
        initialized(newState)
      case PersistEpoch(epochIndex, replyTo) =>
        val (newState, inputsWoAddress) = state.persistEpoch(epochIndex)
        if (inputsWoAddress.nonEmpty)
          logger.error(s"Inputs without address : ${inputsWoAddress.mkString(", ")}")
        logger.info(s"Utxo size : ${newState.byId.size}, addresses count : ${newState.byAddress.size}")
        replyTo ! StatusReply.success(newState) //todo Try
        initialized(newState)
    }

  def addBestBlock(
    bestBlock: Block
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[HolderRequest]): Future[UtxoState] =
    ref.askWithStatus(ref => AddBestBlock(bestBlock, ref))

  def addFork(newFork: List[Block], supersededFork: List[CachedBlock])(implicit
    s: ActorSystem[Nothing],
    ref: ActorRef[HolderRequest]
  ): Future[UtxoState] =
    ref.askWithStatus(ref => AddForkBlock(newFork, supersededFork, ref))

  def persistEpoch(epochIndex: Int)(implicit s: ActorSystem[Nothing], ref: ActorRef[HolderRequest]): Future[UtxoState] =
    ref.askWithStatus(ref => PersistEpoch(epochIndex, ref))

  trait HolderRequest
  case class AddBestBlock(bestBlock: Block, replyTo: ActorRef[StatusReply[UtxoState]]) extends HolderRequest

  case class AddForkBlock(newFork: List[Block], supersededFork: List[CachedBlock], replyTo: ActorRef[StatusReply[UtxoState]])
    extends HolderRequest

  case class PersistEpoch(epochIndex: Int, replyTo: ActorRef[StatusReply[UtxoState]]) extends HolderRequest
}
