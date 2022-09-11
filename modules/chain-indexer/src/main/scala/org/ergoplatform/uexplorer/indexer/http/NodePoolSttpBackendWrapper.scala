package org.ergoplatform.uexplorer.indexer.http

import akka.actor.typed._
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.Resiliency
import org.ergoplatform.uexplorer.indexer.http.MetadataHttpClient._
import org.ergoplatform.uexplorer.indexer.http.NodePool.NodePoolRequest
import sttp.capabilities.Effect
import sttp.client3._
import sttp.model.Uri

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util._

class NodePoolSttpBackendWrapper[P](nodePoolRef: ActorRef[NodePoolRequest], metadataClient: MetadataHttpClient[P])(implicit
  c: ActorSystem[Nothing]
) extends DelegateSttpBackend[Future, P](metadataClient.underlyingB)
  with LazyLogging {
  import NodePool._
  implicit private val timeout: Timeout = 3.seconds

  override def close(): Future[Unit] = {
    logger.info("Closing sttp backend")
    nodePoolRef.tell(GracefulShutdown)
    super.close()
  }

  def recursiveCall[R](
    peerAddresses: List[Uri],
    invalidPeers: Set[Uri] = Set.empty
  )(run: Uri => Future[R], invalidFn: InvalidPeers => Future[_]): Future[R] =
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
        invalidFn(invalidPeers).flatMap(_ =>
          Future.failed(new Resiliency.StopException(s"All peers failed to execute request", null))
        )
      case None =>
        Future.failed(new Resiliency.StopException(s"Should never happen, probably received no peerAddresses", null))
    }

  override def send[T, R >: P with Effect[Future]](origRequest: Request[T, R]): Future[Response[T]] = {

    def sendProxyRequest(peerUri: Uri): Future[Response[T]] = metadataClient.underlyingB.send(origRequest.get(peerUri))

    def invalidatePeers(invalidPeers: InvalidPeers): Future[AllBestPeers] =
      nodePoolRef.ask(ref => InvalidatePeers(invalidPeers.map(stripUri), ref))

    nodePoolRef
      .ask[AllBestPeers](GetAllPeers)
      .flatMap {
        case AllBestPeers(addresses) if addresses.isEmpty =>
          Future.failed(
            new Resiliency.StopException(s"Run out of peers to make http call to, master should be always available", null)
          )
        case AllBestPeers(peerHosts) =>
          recursiveCall(peerHosts.map(copyUri(origRequest.uri, _)).toList)(sendProxyRequest, invalidatePeers)
      }
  }

}

object NodePoolSttpBackendWrapper {

  def apply[P](nodePoolRef: ActorRef[NodePoolRequest], httpClient: MetadataHttpClient[P])(implicit
    s: ActorSystem[Nothing]
  ): NodePoolSttpBackendWrapper[P] =
    new NodePoolSttpBackendWrapper(nodePoolRef, httpClient)

}
