package org.ergoplatform.uexplorer.indexer.http

import akka.NotUsed
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.stream.ActorAttributes
import akka.stream.scaladsl.Source
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.{AkkaStreamSupport, Resiliency}

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
      case GetAllPeers(replyTo) =>
        ctx.scheduleOnce(500.millis, ctx.self, GetAllPeers(replyTo))
        Behaviors.same
      case UpdateOpenApiPeers(validPeers, replyTo) =>
        val newState = NodePoolState(validPeers, Set.empty)
        replyTo ! newState
        initialized(newState)
      case x =>
        logger.error(s"Message unexpected : $x")
        Behaviors.stopped
    }

  def initialized(state: NodePoolState)(implicit
    ctx: ActorContext[NodePoolRequest]
  ): Behavior[NodePoolRequest] =
    Behaviors.receiveMessage[NodePoolRequest] {
      case GetAllPeers(replyTo) if state.openApiPeers.isEmpty =>
        ctx.scheduleOnce(500.millis, ctx.self, GetAllPeers(replyTo))
        Behaviors.same
      case GetAllPeers(replyTo) =>
        replyTo ! state.sortPeers
        Behaviors.same
      case UpdateOpenApiPeers(validPeers, replyTo) =>
        val newState = state.updatePeers(validPeers)
        replyTo ! newState
        initialized(newState)
      case InvalidatePeers(invalidatedPeers, replyTo) =>
        val (newInvalidPeers, newState) = state.invalidatePeers(invalidatedPeers)
        if (newInvalidPeers.nonEmpty)
          logger.info(newState.toString)
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

  case class GetAllPeers(replyTo: ActorRef[AllBestPeers]) extends NodePoolRequest

  case class UpdateOpenApiPeers(validAddresses: Set[Peer], replyTo: ActorRef[NodePoolState]) extends NodePoolRequest

  case class InvalidatePeers(peerAddresses: Set[Peer], replyTo: ActorRef[NodePoolState]) extends NodePoolRequest

  sealed trait NodePoolResponse

  case class AllBestPeers(peerAddresses: List[Peer]) extends NodePoolResponse

  case class NodePoolState(openApiPeers: Set[Peer], invalidPeers: Set[Peer]) {

    def invalidatePeers(invalidatedPeers: Set[Peer]): (Set[Peer], NodePoolState) = {
      val newInvalidPeers = invalidPeers ++ invalidatedPeers
      val newOpenApiPeers = openApiPeers.diff(newInvalidPeers)
      invalidatedPeers.diff(invalidPeers) -> NodePoolState(newOpenApiPeers, newInvalidPeers)
    }

    def updatePeers(validPeers: Set[Peer]): NodePoolState =
      NodePoolState(validPeers.diff(invalidPeers.filter(_.weight > 2)), invalidPeers -- validPeers.filter(_.weight < 3))

    override def toString: String = {
      val validPeersStr = openApiPeers.headOption
        .map(_ => s"Valid peers : ${openApiPeers.mkString(", ")}")
        .getOrElse("No valid peers available")
      val invalidPeersStr =
        invalidPeers.headOption.map(_ => s", invalid peers : ${invalidPeers.mkString(", ")}").getOrElse("")
      s"$validPeersStr$invalidPeersStr"
    }

    def sortPeers: AllBestPeers = AllBestPeers(openApiPeers.toList.sortBy(_.weight))
  }

}
