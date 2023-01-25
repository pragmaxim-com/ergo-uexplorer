package org.ergoplatform.uexplorer.indexer.http

import akka.{Done, NotUsed}
import akka.actor.typed.*
import akka.stream.{ActorAttributes, KillSwitches, SharedKillSwitch}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.{Resiliency, Utils}
import org.ergoplatform.uexplorer.indexer.http.NodePool.*
import org.ergoplatform.uexplorer.indexer.http.SttpNodePoolBackend.swapUri
import sttp.capabilities.Effect
import sttp.client3.*
import sttp.model.Uri
import akka.actor.typed.scaladsl.AskPattern.*

import scala.collection.immutable.{SortedSet, TreeSet}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.*

class SttpNodePoolBackend[P]()(implicit
  sys: ActorSystem[Nothing],
  nodePoolRef: ActorRef[NodePoolRequest],
  underlying: SttpBackend[Future, P],
  killSwitch: SharedKillSwitch
) extends DelegateSttpBackend[Future, P](underlying) {
  import SttpNodePoolBackend.fallbackQuery

  implicit private val timeout: Timeout = 5.seconds

  override def close(): Future[Unit] = {
    nodePoolRef.tell(GracefulShutdown)
    super.close()
  }

  private def updateNodePool(metadataClient: MetadataHttpClient[_]): Future[NodePoolState] =
    metadataClient.getAllOpenApiPeers
      .flatMap { validPeers =>
        nodePoolRef.ask(ref => UpdateOpenApiPeers(validPeers, ref))
      }

  def keepNodePoolUpdated(metadataClient: MetadataHttpClient[_]): Future[NodePoolState] =
    updateNodePool(metadataClient)
      .andThen { case Success(_) =>
        schedule(15.seconds, 30.seconds)(updateNodePool(metadataClient)).via(killSwitch.flow).run()
      }

  override def send[T, R >: P with Effect[Future]](origRequest: Request[T, R]): Future[Response[T]] = {

    def proxy(peer: Peer): Future[Response[T]] =
      underlying.send(swapUri(origRequest, peer.uri))

    NodePool.getAvailablePeers
      .flatMap {
        case AvailablePeers(peers) if peers.isEmpty =>
          Future.failed(new Exception(s"Run out of peers to make http call to, master should be always available", null))
        case AvailablePeers(peers) =>
          fallbackQuery(peers)(proxy).flatMap {
            case (invalidPeers, blockTry) if invalidPeers.nonEmpty =>
              NodePool.invalidatePeers(invalidPeers).flatMap(_ => Future.fromTry(blockTry))
            case (_, blockTry) =>
              Future.fromTry(blockTry)
          }
      }
  }

}

object SttpNodePoolBackend extends LazyLogging {
  type InvalidPeers = SortedSet[Peer]

  def apply[P](nodePoolRef: ActorRef[NodePoolRequest])(implicit
    s: ActorSystem[Nothing],
    underlyingBackend: SttpBackend[Future, P],
    killSwitch: SharedKillSwitch
  ): SttpNodePoolBackend[P] =
    new SttpNodePoolBackend()(s, nodePoolRef, underlyingBackend, killSwitch)

  def swapUri[T, R](reqWithDummyUri: Request[T, R], peerUri: Uri): Request[T, R] =
    reqWithDummyUri.get(Utils.copyUri(reqWithDummyUri.uri, peerUri))

  def fallbackQuery[R](
    peerAddresses: List[Peer],
    invalidPeers: SortedSet[Peer] = TreeSet.empty
  )(run: Peer => Future[R]): Future[(InvalidPeers, Try[R])] =
    peerAddresses.headOption match {
      case Some(peerAddress) =>
        run(peerAddress).transformWith {
          case Success(block) =>
            Future.successful(invalidPeers -> Success(block))
          case Failure(ex) =>
            if (peerAddresses.tail.nonEmpty) {
              logger.warn(s"Peer $peerAddress failed, retrying with another peer...", ex)
            }
            fallbackQuery(peerAddresses.tail, invalidPeers + peerAddresses.head)(run)
        }
      case None =>
        logger.warn(s"We ran out of peers, all peers unavailable...")
        Future.successful(invalidPeers, Failure(new Exception("Run out of peers!")))
    }

}
