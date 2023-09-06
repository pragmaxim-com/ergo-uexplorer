package org.ergoplatform.uexplorer.http

import org.ergoplatform.uexplorer.http.NodePool.{InvalidPeers, NewPeers}
import zio.*

import scala.collection.immutable.{SortedSet, TreeSet}
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

case class NodePool(ref: Ref[NodePoolState]) {
  def updateOpenApiPeers(peers: SortedSet[Peer]): UIO[NewPeers] = ref.modify(_.updatePeers(peers))
  def invalidatePeers(peers: InvalidPeers): UIO[InvalidPeers]   = ref.modify(_.invalidatePeers(peers))

  def getAvailablePeers: UIO[SortedSet[Peer]] = ref.get.map(_.openApiPeers)
  def clean: UIO[Unit]                        = ref.set(NodePoolState.empty)
}

object NodePool {
  type InvalidPeers = SortedSet[Peer]
  type NewPeers     = SortedSet[Peer]
  type AllPeers     = SortedSet[Peer]
  def layer: ZLayer[Any, Nothing, NodePool] = ZLayer.fromZIO(Ref.make(NodePoolState.empty).map(NodePool(_)))
}
