package org.ergoplatform.uexplorer.http

import akka.actor.typed.scaladsl.AskPattern.*
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable.{SortedSet, TreeSet}
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

object NodePool extends LazyLogging {

  implicit private val timeout: Timeout = 3.seconds

  def behavior: Behavior[NodePoolRequest] =
    Behaviors.receiveMessage[NodePoolRequest] {
      case UpdateOpenApiPeers(validPeers, replyTo) =>
        val newState = NodePoolState(validPeers, TreeSet.empty)
        logger.info(s"$newState")
        replyTo ! newState
        initialized(newState)
      case x =>
        logger.error(s"Message unexpected : $x")
        Behaviors.stopped
    }

  def initialized(state: NodePoolState): Behavior[NodePoolRequest] =
    Behaviors.receiveMessage[NodePoolRequest] {
      case GetAvailablePeers(replyTo) =>
        replyTo ! state.sortPeers
        Behaviors.same
      case UpdateOpenApiPeers(validPeers, replyTo) =>
        val newState = state.updatePeers(validPeers)
        replyTo ! newState
        initialized(newState)
      case InvalidatePeers(invalidatedPeers, replyTo) =>
        val (newInvalidPeers, newState) = state.invalidatePeers(invalidatedPeers)
        if (newInvalidPeers.nonEmpty)
          logger.info(s"Getting blocks from : $newState")
        replyTo ! newState
        initialized(newState)
      case GracefulShutdown =>
        logger.info(s"Stopping NodePool")
        Behaviors.stopped
    }

  sealed trait NodePoolRequest

  case object GracefulShutdown extends NodePoolRequest

  case class GetAvailablePeers(replyTo: ActorRef[AvailablePeers]) extends NodePoolRequest

  case class UpdateOpenApiPeers(validAddresses: SortedSet[Peer], replyTo: ActorRef[NodePoolState]) extends NodePoolRequest

  case class InvalidatePeers(peerAddresses: Set[Peer], replyTo: ActorRef[NodePoolState]) extends NodePoolRequest

  sealed trait NodePoolResponse

  case class AvailablePeers(peerAddresses: List[Peer]) extends NodePoolResponse

  type InvalidPeers = SortedSet[Peer]

  def getAvailablePeers(implicit s: ActorSystem[Nothing], actorRef: ActorRef[NodePoolRequest]): Future[AvailablePeers] =
    actorRef.ask[AvailablePeers](ref => GetAvailablePeers(ref))

  def invalidatePeers(
    invalidPeers: InvalidPeers
  )(implicit s: ActorSystem[Nothing], actorRef: ActorRef[NodePoolRequest]): Future[NodePoolState] =
    actorRef.ask(ref => InvalidatePeers(invalidPeers, ref))
}
