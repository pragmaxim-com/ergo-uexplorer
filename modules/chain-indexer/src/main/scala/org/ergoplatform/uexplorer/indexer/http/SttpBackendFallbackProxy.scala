package org.ergoplatform.uexplorer.indexer.http

import akka.actor.typed._
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.Utils
import org.ergoplatform.uexplorer.indexer.http.NodePool._
import org.ergoplatform.uexplorer.indexer.http.SttpBackendFallbackProxy.swapUri
import sttp.capabilities.Effect
import sttp.client3._
import sttp.model.Uri

import scala.collection.immutable.{SortedSet, TreeSet}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util._

class SttpBackendFallbackProxy[P](nodePoolRef: ActorRef[NodePoolRequest])(implicit
  c: ActorSystem[Nothing],
  underlying: SttpBackend[Future, P]
) extends DelegateSttpBackend[Future, P](underlying) {
  import SttpBackendFallbackProxy.fallbackQuery

  implicit private val timeout: Timeout = 5.seconds

  override def close(): Future[Unit] = {
    nodePoolRef.tell(GracefulShutdown)
    super.close()
  }

  override def send[T, R >: P with Effect[Future]](origRequest: Request[T, R]): Future[Response[T]] = {

    def proxy(peer: Peer): Future[Response[T]] =
      underlying.send(swapUri(origRequest, peer.uri))

    nodePoolRef
      .ask[AvailablePeers](GetAvailablePeers)
      .flatMap {
        case AvailablePeers(peers) if peers.isEmpty =>
          Future.failed(new Exception(s"Run out of peers to make http call to, master should be always available", null))
        case AvailablePeers(peers) =>
          fallbackQuery(peers)(proxy).flatMap {
            case (invalidPeers, blockTry) if invalidPeers.nonEmpty =>
              nodePoolRef
                .ask(ref => InvalidatePeers(invalidPeers, ref))
                .flatMap(_ => Future.fromTry(blockTry))
            case (_, blockTry) =>
              Future.fromTry(blockTry)
          }
      }
  }

}

object SttpBackendFallbackProxy extends LazyLogging {
  type InvalidPeers = SortedSet[Peer]

  def apply[P](nodePoolRef: ActorRef[NodePoolRequest], httpClient: MetadataHttpClient[P])(implicit
    s: ActorSystem[Nothing]
  ): SttpBackendFallbackProxy[P] =
    new SttpBackendFallbackProxy(nodePoolRef)(s, httpClient.underlyingB)

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
        logger.warn(s"We ran out of peers, all peers unavailable : ${invalidPeers.mkString(",")}")
        Future.successful(invalidPeers, Failure(new Exception("Run out of peers!")))
    }

}
