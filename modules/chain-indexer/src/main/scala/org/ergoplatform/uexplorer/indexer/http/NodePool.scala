package org.ergoplatform.uexplorer.indexer.http

import akka.NotUsed
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.stream.ActorAttributes
import akka.stream.scaladsl.Source
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.{AkkaStreamSupport, PeerAddress, Resiliency, Utils}
import sttp.model.Uri

import scala.concurrent.duration.DurationInt
import scala.util.Random

object NodePool extends AkkaStreamSupport with LazyLogging {

  implicit private val timeout: Timeout = 3.seconds

  def initialBehavior(metadataClient: MetadataHttpClient[_]): Behavior[NodePoolRequest] =
    Behaviors.setup[NodePoolRequest] { implicit ctx =>
      implicit val s: ActorSystem[Nothing] = ctx.system
      nodePoolUpdateSource(ctx.self, metadataClient).run()
      initialized(NodePoolState.empty(metadataClient.masterPeerAddr))
    }

  def initialized(state: NodePoolState)(implicit
    ctx: ActorContext[NodePoolRequest]
  ): Behavior[NodePoolRequest] =
    Behaviors.receiveMessage[NodePoolRequest] {
      case GetAllPeers(replyTo) if state.openApiPeers.isEmpty =>
        ctx.scheduleOnce(500.millis, ctx.self, GetAllPeers(replyTo))
        Behaviors.same
      case GetAnyBestPeer(replyTo) if state.openApiPeers.isEmpty =>
        ctx.scheduleOnce(500.millis, ctx.self, GetAnyBestPeer(replyTo))
        Behaviors.same
      case GetAllPeers(replyTo) =>
        replyTo ! state.shufflePeers
        Behaviors.same
      case GetAnyBestPeer(replyTo) =>
        replyTo ! state.anyBestPeer
        Behaviors.same
      case UpdateOpenApiPeers(validPeers, addedInvalidPeers, replyTo) =>
        val newState = state.updatePeers(validPeers, addedInvalidPeers)
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
        .mapAsync(1)(_ => metadataClient.getMasterInfo)
        .mapAsync(1)(peerInfo => metadataClient.getValidAndInvalidPeers(peerInfo))
        .mapAsync(1) { case (validPeers, invalidPeers) =>
          nodePool.ask(ref => UpdateOpenApiPeers(validPeers, invalidPeers, ref))
        }
        .withAttributes(ActorAttributes.supervisionStrategy(Resiliency.decider))
    }

  type InvalidPeers = Set[Uri]

  case class ConnectedPeer(address: PeerAddress, restApiUrl: Option[PeerAddress])

  case class PeerInfo(appVersion: String, stateType: String, fullHeight: Option[Int], restApiUrl: Option[PeerAddress])

  sealed trait NodePoolRequest

  case object GracefulShutdown extends NodePoolRequest

  case class GetAnyBestPeer(replyTo: ActorRef[AnyBestPeer]) extends NodePoolRequest

  case class GetAllPeers(replyTo: ActorRef[AllBestPeers]) extends NodePoolRequest

  case class UpdateOpenApiPeers(validAddresses: Set[Uri], invalidAddresses: Set[Uri], replyTo: ActorRef[NodePoolState])
    extends NodePoolRequest

  case class InvalidatePeers(peerAddresses: Set[Uri], replyTo: ActorRef[NodePoolState]) extends NodePoolRequest

  sealed trait NodePoolResponse

  case class AllBestPeers(peerAddresses: IndexedSeq[Uri]) extends NodePoolResponse

  case class AnyBestPeer(peerAddress: Uri) extends NodePoolResponse

  case class NodePoolState(openApiPeers: Set[Uri], invalidPeers: Set[Uri], masterPeerAddr: Uri) {

    def invalidatePeers(invalidatedPeers: Set[Uri]): (Set[Uri], NodePoolState) = {
      val newInvalidPeers = (invalidPeers ++ invalidatedPeers) - masterPeerAddr
      val newOpenApiPeers = openApiPeers.diff(newInvalidPeers)
      invalidatedPeers.diff(invalidPeers) -> NodePoolState(newOpenApiPeers, newInvalidPeers, masterPeerAddr)
    }

    def updatePeers(validPeers: Set[Uri], addedInvalidPeers: Set[Uri]): NodePoolState = {
      val newInvalidPeers = (invalidPeers ++ addedInvalidPeers) - masterPeerAddr
      val newOpenApiPeers = (validPeers -- newInvalidPeers) + masterPeerAddr
      NodePoolState(newOpenApiPeers, newInvalidPeers, masterPeerAddr)
    }

    override def toString: String = {
      val validPeersStr = openApiPeers.headOption
        .map(_ => s"Valid peers : ${openApiPeers.mkString(", ")}")
        .getOrElse("No valid peers available")
      val invalidPeersStr =
        invalidPeers.headOption.map(_ => s", invalid peers : ${invalidPeers.mkString(", ")}").getOrElse("")
      s"$validPeersStr$invalidPeersStr"
    }

    def anyBestPeer: AnyBestPeer = AnyBestPeer(openApiPeers.toVector(Random.nextInt(openApiPeers.size)))

    def shufflePeers: AllBestPeers = AllBestPeers(Utils.shuffle(openApiPeers.toArray))
  }

  object NodePoolState {
    def empty(masterPeerAddr: Uri): NodePoolState = NodePoolState(Set.empty, Set.empty, masterPeerAddr)
  }

}
