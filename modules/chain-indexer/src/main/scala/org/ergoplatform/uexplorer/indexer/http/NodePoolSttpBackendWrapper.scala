package org.ergoplatform.uexplorer.indexer.http

import akka.actor.typed._
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.http.NodePool._
import org.ergoplatform.uexplorer.indexer.http.NodePoolSttpBackendWrapper.InvalidPeers
import org.ergoplatform.uexplorer.indexer.{StopException, Utils}
import sttp.capabilities.Effect
import sttp.client3._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util._

class NodePoolSttpBackendWrapper[P](nodePoolRef: ActorRef[NodePoolRequest])(implicit
  c: ActorSystem[Nothing],
  underlying: SttpBackend[Future, P]
) extends DelegateSttpBackend[Future, P](underlying)
  with LazyLogging {
  implicit private val timeout: Timeout = 5.seconds

  override def close(): Future[Unit] = {
    logger.info("Closing sttp backend")
    nodePoolRef.tell(GracefulShutdown)
    super.close()
  }

  def recursiveCall[R](
    peerAddresses: List[Peer],
    invalidPeers: Set[Peer] = Set.empty
  )(run: Peer => Future[R], invalidFn: InvalidPeers => Future[_]): Future[R] =
    peerAddresses.headOption match {
      case Some(peerAddress) =>
        run(peerAddress).transformWith {
          case Success(block) if invalidPeers.nonEmpty =>
            invalidFn(invalidPeers).map(_ => block)
          case Success(block) =>
            Future.successful(block)
          case Failure(ex) =>
            if (peerAddresses.nonEmpty) {
              logger.warn(s"Peer $peerAddress failed, retrying with another peer ${peerAddresses.head}", ex)
            }
            recursiveCall(peerAddresses.tail, invalidPeers + peerAddresses.head)(run, invalidFn)
        }
      case None if invalidPeers.nonEmpty =>
        invalidFn(invalidPeers).flatMap(_ => Future.failed(new StopException(s"All peers failed to execute request", null)))
      case None =>
        Future.failed(new StopException(s"Should never happen, probably received no peerAddresses", null))
    }

  override def send[T, R >: P with Effect[Future]](origRequest: Request[T, R]): Future[Response[T]] = {

    def sendProxyRequest(peerUri: Peer): Future[Response[T]] =
      underlying.send(origRequest.get(Utils.copyUri(origRequest.uri, peerUri.uri)))

    def invalidatePeers(invalidPeers: InvalidPeers): Future[NodePoolState] =
      nodePoolRef.ask(ref => InvalidatePeers(invalidPeers, ref))

    nodePoolRef
      .ask[AllBestPeers](GetAllPeers)
      .flatMap {
        case AllBestPeers(peers) if peers.isEmpty =>
          Future.failed(
            new StopException(s"Run out of peers to make http call to, master should be always available", null)
          )
        case AllBestPeers(peers) =>
          recursiveCall(peers)(sendProxyRequest, invalidatePeers)
      }
  }

}

object NodePoolSttpBackendWrapper {
  type InvalidPeers = Set[Peer]

  def apply[P](nodePoolRef: ActorRef[NodePoolRequest], httpClient: MetadataHttpClient[P])(implicit
    s: ActorSystem[Nothing]
  ): NodePoolSttpBackendWrapper[P] =
    new NodePoolSttpBackendWrapper(nodePoolRef)(s, httpClient.underlyingB)

}
