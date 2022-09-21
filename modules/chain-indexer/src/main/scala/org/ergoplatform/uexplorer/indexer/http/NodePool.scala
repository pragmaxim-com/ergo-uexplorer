package org.ergoplatform.uexplorer.indexer.http

import akka.NotUsed
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.stream.ActorAttributes
import akka.stream.scaladsl.Source
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.http.SttpBackendFallbackProxy.InvalidPeers
import org.ergoplatform.uexplorer.indexer.{AkkaStreamSupport, Resiliency}

import scala.collection.immutable.{SortedSet, TreeSet}
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

object NodePool extends AkkaStreamSupport with LazyLogging {

  implicit private val timeout: Timeout = 3.seconds

  def behavior(metadataClient: MetadataHttpClient[_]): Behavior[NodePoolRequest] =
    Behaviors.setup[NodePoolRequest] { implicit ctx =>
      implicit val s: ActorSystem[Nothing] = ctx.system
      nodePoolUpdateSource(ctx.self, metadataClient).run()
      uninitialized
    }

  def uninitialized(implicit ctx: ActorContext[NodePoolRequest]): Behavior[NodePoolRequest] =
    Behaviors.receiveMessage[NodePoolRequest] {
      case GetAvailablePeers(replyTo) =>
        ctx.scheduleOnce(500.millis, ctx.self, GetAvailablePeers(replyTo))
        Behaviors.same
      case UpdateOpenApiPeers(validPeers, replyTo) =>
        val newState = NodePoolState(validPeers, TreeSet.empty)
        logger.info(s"Getting blocks from : $newState")
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
        Behaviors.stopped
    }

  def nodePoolUpdateSource(nodePool: ActorRef[NodePoolRequest], metadataClient: MetadataHttpClient[_])(implicit
    s: ActorSystem[_]
  ): Source[NodePoolState, NotUsed] =
    restartSource {
      Source
        .tick(0.seconds, 30.seconds, ())
        .mapAsync(1)(_ => metadataClient.getAllOpenApiPeers)
        .mapAsync(1) { validPeers =>
          nodePool.ask(ref => UpdateOpenApiPeers(validPeers, ref))
        }
        .withAttributes(ActorAttributes.supervisionStrategy(Resiliency.decider))
    }

  sealed trait NodePoolRequest

  case object GracefulShutdown extends NodePoolRequest

  case class GetAvailablePeers(replyTo: ActorRef[AvailablePeers]) extends NodePoolRequest

  case class UpdateOpenApiPeers(validAddresses: SortedSet[Peer], replyTo: ActorRef[NodePoolState]) extends NodePoolRequest

  case class InvalidatePeers(peerAddresses: Set[Peer], replyTo: ActorRef[NodePoolState]) extends NodePoolRequest

  sealed trait NodePoolResponse

  case class AvailablePeers(peerAddresses: List[Peer]) extends NodePoolResponse

  def getAvailablePeers(implicit s: ActorSystem[Nothing], actorRef: ActorRef[NodePoolRequest]): Future[AvailablePeers] =
    actorRef.ask[AvailablePeers](GetAvailablePeers)

  def invalidatePeers(
    invalidPeers: InvalidPeers
  )(implicit s: ActorSystem[Nothing], actorRef: ActorRef[NodePoolRequest]): Future[NodePoolState] =
    actorRef.ask(ref => InvalidatePeers(invalidPeers, ref))
}
