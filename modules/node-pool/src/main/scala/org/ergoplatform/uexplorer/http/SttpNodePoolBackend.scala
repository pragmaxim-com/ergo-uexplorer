package org.ergoplatform.uexplorer.http

import nl.vroste.rezilience.Retry.Schedules
import org.ergoplatform.uexplorer.Utils
import org.ergoplatform.uexplorer.http.NodePool.*
import org.ergoplatform.uexplorer.http.SttpNodePoolBackend.{swapUri, NodePoolSchedule}
import sttp.capabilities.Effect
import sttp.client3.*
import sttp.model.Uri
import zio.*

import scala.collection.immutable.{SortedSet, TreeSet}
import scala.util.*
import sttp.capabilities.zio.ZioStreams

case class SttpNodePoolBackend(schedule: NodePoolSchedule)(
  underlyingBackend: UnderlyingBackend,
  metadataClient: MetadataHttpClient,
  nodePool: NodePool
) extends DelegateSttpBackend[Task, ZioStreams](underlyingBackend.backend) {
  import SttpNodePoolBackend.fallbackQuery

  override def close(): Task[Unit] =
    nodePool.clean *> super.close()

  private def updateNodePool: Task[AllPeers] =
    for
      allPeers <- metadataClient.getAllOpenApiPeers
      newPeers <- nodePool.updateOpenApiPeers(allPeers)
      _        <- ZIO.when(newPeers.nonEmpty)(ZIO.log(s"New peers connected : ${newPeers.mkString(",")}"))
    yield allPeers

  def cleanNodePool: Task[Unit] = nodePool.clean

  def keepNodePoolUpdated: Task[Fiber.Runtime[Throwable, Long]] =
    for
      allPeers <- updateNodePool
      _        <- ZIO.log(s"${allPeers.size} peer(s) available : ${allPeers.mkString(",")}")
      fiber    <- updateNodePool.repeat(Schedule.fixed(30.seconds)).fork
    yield fiber

  override def send[T, R >: ZioStreams with Effect[Task]](origRequest: Request[T, R]): Task[Response[T]] = {

    def proxy(peer: Peer): Task[Response[T]] =
      underlyingBackend.backend.send(swapUri(origRequest, peer.uri))

    nodePool.getAvailablePeers
      .flatMap {
        case peers if peers.isEmpty =>
          ZIO.fail(new Exception(s"Ran out of peers to make http call to, master should be always available", null))
        case peers =>
          fallbackQuery(peers.toList.zipWithIndex, schedule)(proxy).flatMap {
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
  case class NodePoolSchedule(schedule: Option[Schedule[Any, Any, (Any, Long)]])

  def layer: ZLayer[UnderlyingBackend with MetadataHttpClient with NodePool, Nothing, SttpNodePoolBackend] =
    ZLayer.fromFunction(SttpNodePoolBackend(NodePoolSchedule(Some(Schedules.common(1.second, 5.seconds, 2.0, true, Some(10))))).apply _)

  def layerWithNoSchedule: ZLayer[UnderlyingBackend with MetadataHttpClient with NodePool, Nothing, SttpNodePoolBackend] =
    ZLayer.fromFunction(SttpNodePoolBackend(NodePoolSchedule(None)).apply _)

  def swapUri[T, R](reqWithDummyUri: Request[T, R], peerUri: Uri): Request[T, R] =
    reqWithDummyUri.get(Utils.copyUri(reqWithDummyUri.uri, peerUri))

  def fallbackQuery[R](
    peerAddressesWithIndex: List[(Peer, Int)],
    schedule: NodePoolSchedule,
    invalidPeers: SortedSet[Peer] = TreeSet.empty
  )(run: Peer => Task[R]): Task[(NodePool.InvalidPeers, Task[R])] =
    peerAddressesWithIndex.headOption match {
      case Some((peerAddress, index)) =>
        val execution =
          if (index == 0 && schedule.schedule.isDefined)
            run(peerAddress).retry(schedule.schedule.get)
          else
            run(peerAddress)

        execution
          .map { block =>
            invalidPeers -> ZIO.succeed(block)
          }
          .catchNonFatalOrDie { ex =>
            ZIO.logErrorCause(s"Peer $peerAddress failed, retrying with another peer...", Cause.fail(ex)) *> fallbackQuery(
              peerAddressesWithIndex.tail,
              schedule,
              invalidPeers + peerAddressesWithIndex.head._1
            )(run)
          }
      case None =>
        ZIO.logWarning(s"We ran out of peers, all peers unavailable...") *> ZIO.succeed(
          invalidPeers -> ZIO.fail(new Exception("Run out of peers!"))
        )
    }

}
