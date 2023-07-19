package org.ergoplatform.uexplorer.http

import org.ergoplatform.uexplorer.Utils
import org.ergoplatform.uexplorer.http.NodePool.*
import org.ergoplatform.uexplorer.http.SttpNodePoolBackend.swapUri
import sttp.capabilities.Effect
import sttp.client3.*
import sttp.model.Uri
import zio.*

import scala.collection.immutable.{SortedSet, TreeSet}
import scala.util.*
import sttp.capabilities.zio.ZioStreams
import sttp.client3.httpclient.zio.{HttpClientZioBackend, SttpClient}

case class SttpNodePoolBackend(
  underlyingBackend: UnderlyingBackend,
  metadataClient: MetadataHttpClient,
  nodePool: NodePool
) extends DelegateSttpBackend[Task, ZioStreams](underlyingBackend.backend) {
  import SttpNodePoolBackend.fallbackQuery

  override def close(): Task[Unit] =
    nodePool.clean *> super.close()

  private def updateNodePool: Task[Unit] =
    for
      allPeers <- metadataClient.getAllOpenApiPeers
      newPeers <- nodePool.updateOpenApiPeers(allPeers)
      _        <- ZIO.log(s"New peers added : ${newPeers.mkString(",")}")
    yield ()

  def keepNodePoolUpdated: ZIO[Any, Throwable, Fiber.Runtime[Throwable, Long]] =
    for
      done  <- updateNodePool
      fiber <- ZIO.scoped(updateNodePool.scheduleFork(Schedule.fixed(30.seconds)))
    yield fiber

  override def send[T, R >: ZioStreams with Effect[Task]](origRequest: Request[T, R]): Task[Response[T]] = {

    def proxy(peer: Peer): Task[Response[T]] =
      underlyingBackend.backend.send(swapUri(origRequest, peer.uri))

    nodePool.getAvailablePeers
      .flatMap {
        case peers if peers.isEmpty =>
          ZIO.fail(new Exception(s"Ran out of peers to make http call to, master should be always available", null))
        case peers =>
          fallbackQuery(peers.toList)(proxy).flatMap {
            case (invalidPeers, blockTry) if invalidPeers.nonEmpty =>
              nodePool
                .invalidatePeers(invalidPeers)
                .tap(invalidPeers => ZIO.log(s"Peers invalidated : ${invalidPeers.mkString(",")}")) *> blockTry
            case (_, blockTry) =>
              blockTry
          }
      }
  }

}

object SttpNodePoolBackend {

  def layer: ZLayer[UnderlyingBackend with MetadataHttpClient with NodePool, Nothing, SttpNodePoolBackend] =
    ZLayer.fromFunction(SttpNodePoolBackend.apply _)

  def swapUri[T, R](reqWithDummyUri: Request[T, R], peerUri: Uri): Request[T, R] =
    reqWithDummyUri.get(Utils.copyUri(reqWithDummyUri.uri, peerUri))

  def fallbackQuery[R](
    peerAddresses: List[Peer],
    invalidPeers: SortedSet[Peer] = TreeSet.empty
  )(run: Peer => Task[R]): Task[(NodePool.InvalidPeers, Task[R])] =
    peerAddresses.headOption match {
      case Some(peerAddress) =>
        run(peerAddress)
          .map { block =>
            invalidPeers -> ZIO.succeed(block)
          }
          .catchNonFatalOrDie { ex =>
            ZIO.logErrorCause(s"Peer $peerAddress failed, retrying with another peer...", Cause.fail(ex)) *> fallbackQuery(
              peerAddresses.tail,
              invalidPeers + peerAddresses.head
            )(run)
          }
      case None =>
        ZIO.logWarning(s"We ran out of peers, all peers unavailable...") *> ZIO.succeed(
          invalidPeers -> ZIO.fail(new Exception("Run out of peers!"))
        )
    }

}
