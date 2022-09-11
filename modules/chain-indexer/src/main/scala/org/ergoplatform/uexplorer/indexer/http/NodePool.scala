package org.ergoplatform.uexplorer.indexer.http

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{RestartSource, Source}
import akka.util.Timeout
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.{AkkaStreamSupport, PeerAddress, Resiliency}
import sttp.model.Uri
import akka.actor.typed.scaladsl.AskPattern._

import scala.collection.compat.immutable.ArraySeq
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Random

object NodePool extends AkkaStreamSupport with LazyLogging {
  type InvalidPeers = Set[Uri]

  implicit private val timeout: Timeout = 3.seconds

  case class ConnectedPeer(address: PeerAddress, restApiUrl: Option[PeerAddress])

  case class PeerInfo(appVersion: String, stateType: String, fullHeight: Option[Int], restApiUrl: Option[PeerAddress])

  sealed trait NodePoolRequest

  case object GracefulShutdown extends NodePoolRequest

  case class GetAnyBestPeer(replyTo: ActorRef[AnyBestPeer]) extends NodePoolRequest

  case class GetAllPeers(replyTo: ActorRef[AllBestPeers]) extends NodePoolRequest

  case class UpdateOpenApiPeers(validAddresses: Set[Uri], invalidAddresses: Set[Uri], replyTo: ActorRef[AllPeers])
    extends NodePoolRequest

  case class InvalidatePeers(peerAddresses: Set[Uri], replyTo: ActorRef[AllBestPeers]) extends NodePoolRequest

  sealed trait NodePoolResponse

  case class AllPeers(valid: IndexedSeq[Uri], invalid: List[Uri]) extends NodePoolResponse

  case class AllBestPeers(peerAddresses: IndexedSeq[Uri]) extends NodePoolResponse

  case class AnyBestPeer(peerAddress: Uri) extends NodePoolResponse

  def initialBehavior(nodePoolClient: MetadataHttpClient[_]): Behavior[NodePoolRequest] =
    Behaviors.setup[NodePoolRequest] { implicit ctx =>
      implicit val s: ActorSystem[Nothing] = ctx.system
      nodePoolUpdateSource(ctx.self, nodePoolClient).run()
      initialized(Vector.empty, Set.empty, nodePoolClient)
    }

  def shuffle[T](arr: Array[T]): Vector[T] = {
    def swap(i1: Int, i2: Int): Unit = {
      val tmp = arr(i1)
      arr(i1) = arr(i2)
      arr(i2) = tmp
    }

    for (n <- arr.length to 2 by -1) {
      val k = Random.nextInt(n)
      swap(n - 1, k)
    }

    arr.toVector
  }

  def initialized(openApiPeers: IndexedSeq[Uri], invalidPeers: Set[Uri], metadataClient: MetadataHttpClient[_])(implicit
    ctx: ActorContext[NodePoolRequest]
  ): Behavior[NodePoolRequest] =
    Behaviors.receiveMessage[NodePoolRequest] {
      case GetAllPeers(replyTo) if openApiPeers.isEmpty =>
        ctx.scheduleOnce(500.millis, ctx.self, GetAllPeers(replyTo))
        Behaviors.same
      case GetAnyBestPeer(replyTo) if openApiPeers.isEmpty =>
        ctx.scheduleOnce(500.millis, ctx.self, GetAnyBestPeer(replyTo))
        Behaviors.same
      case GetAllPeers(replyTo) =>
        replyTo ! AllBestPeers(shuffle(openApiPeers.toArray))
        Behaviors.same
      case GetAnyBestPeer(replyTo) =>
        replyTo ! AnyBestPeer(openApiPeers(Random.nextInt(openApiPeers.length)))
        Behaviors.same
      case UpdateOpenApiPeers(validPeers, addedInvalidPeers, replyTo) =>
        val newOpenApiPeers = ((validPeers -- invalidPeers) + metadataClient.masterPeerAddr).toVector
        val newInvalidPeers = (invalidPeers ++ addedInvalidPeers) - metadataClient.masterPeerAddr
        replyTo ! AllPeers(newOpenApiPeers, newInvalidPeers.toList)
        initialized(newOpenApiPeers, newInvalidPeers, metadataClient)
      case InvalidatePeers(invalidatedPeers, replyTo) =>
        val newInvalidPeers = (invalidPeers ++ invalidatedPeers) - metadataClient.masterPeerAddr
        val newOpenApiPeers = openApiPeers.filterNot(newInvalidPeers.contains)
        if (invalidatedPeers.diff(invalidPeers).nonEmpty) {
          val validPeersStr = newOpenApiPeers.headOption
            .map(_ => s"Valid peers : ${newOpenApiPeers.mkString(", ")}")
            .getOrElse("No peers available")
          val invalidPeersStr =
            newInvalidPeers.headOption.map(_ => s", invalid peers : ${newInvalidPeers.mkString(", ")}").getOrElse("")
          logger.info(s"$validPeersStr$invalidPeersStr")
        }
        replyTo ! AllBestPeers(newOpenApiPeers)
        initialized(newOpenApiPeers, newInvalidPeers, metadataClient)
      case GracefulShutdown =>
        Behaviors.stopped
    }

  def nodePoolUpdateSource(nodePool: ActorRef[NodePoolRequest], metadataClient: MetadataHttpClient[_])(implicit
    s: ActorSystem[_]
  ): Source[AllPeers, NotUsed] =
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

}
