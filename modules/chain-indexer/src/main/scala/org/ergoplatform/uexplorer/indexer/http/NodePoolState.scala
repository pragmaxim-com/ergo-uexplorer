package org.ergoplatform.uexplorer.indexer.http

import org.ergoplatform.uexplorer.indexer.http.NodePool._

import scala.collection.immutable.SortedSet

case class NodePoolState(openApiPeers: SortedSet[Peer], invalidPeers: SortedSet[Peer]) {

  def invalidatePeers(invalidatedPeers: Set[Peer]): (Set[Peer], NodePoolState) = {
    val newInvalidPeers = invalidPeers ++ invalidatedPeers
    val newOpenApiPeers = openApiPeers.diff(newInvalidPeers)
    invalidatedPeers.diff(invalidPeers) -> NodePoolState(newOpenApiPeers, newInvalidPeers)
  }

  def updatePeers(validPeers: SortedSet[Peer]): NodePoolState =
    NodePoolState(validPeers.diff(invalidPeers.filter(_.weight > 2)), invalidPeers -- validPeers.filter(_.weight < 3))

  override def toString: String = {
    val validPeersStr = openApiPeers.headOption
      .map(_ => s"Valid peers : ${openApiPeers.mkString(", ")}")
      .getOrElse("No valid peers available")
    val invalidPeersStr =
      invalidPeers.headOption.map(_ => s", invalid peers : ${invalidPeers.mkString(", ")}").getOrElse("")
    s"$validPeersStr$invalidPeersStr"
  }

  def sortPeers: AvailablePeers = AvailablePeers(openApiPeers.toList)
}
