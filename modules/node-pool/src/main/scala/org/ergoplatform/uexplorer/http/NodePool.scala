package org.ergoplatform.uexplorer.http

import com.typesafe.scalalogging.LazyLogging
import zio.*

import scala.collection.immutable.{SortedSet, TreeSet}
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

case class NodePool(ref: Ref[NodePoolState]) extends LazyLogging {
  def updateOpenApiPeers(peers: SortedSet[Peer]): UIO[Unit] = ref.update(_.updatePeers(peers))
  def invalidatePeers(peers: Set[Peer]): UIO[Unit] = ref.update { s =>
    val (newInvalidPeers, newState) = s.invalidatePeers(peers)
    if (newInvalidPeers.nonEmpty)
      logger.info(s"Getting blocks from : $newState")
    newState
  }
  def getAvailablePeers: UIO[SortedSet[Peer]] = ref.get.map(_.openApiPeers)
  def clean: UIO[Unit]                        = ref.set(NodePoolState.empty)
}

object NodePool {
  type InvalidPeers = SortedSet[Peer]
  def layer: ZLayer[Any, Nothing, NodePool] = ZLayer.fromZIO(Ref.make(NodePoolState.empty).map(NodePool(_)))
}
