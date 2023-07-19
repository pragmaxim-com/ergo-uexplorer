package org.ergoplatform.uexplorer.http

import org.ergoplatform.uexplorer.http.NodePool.*

import scala.collection.immutable.{SortedSet, TreeSet}

case class NodePoolState(openApiPeers: SortedSet[Peer], invalidPeers: SortedSet[Peer]) {

  def invalidatePeers(invalidatedPeers: InvalidPeers): (InvalidPeers, NodePoolState) = {
    val newInvalidPeers = invalidPeers ++ invalidatedPeers
    val newOpenApiPeers = openApiPeers.diff(newInvalidPeers)
    invalidatedPeers.diff(invalidPeers) -> NodePoolState(newOpenApiPeers, newInvalidPeers)
  }

  def updatePeers(validPeers: SortedSet[Peer]): (NewPeers, NodePoolState) = {
    val newPeersWoInvalidPeers = validPeers.diff(invalidPeers.filter(_.weight > 2))
    val newPeersAdded          = newPeersWoInvalidPeers.diff(openApiPeers)
    newPeersAdded -> NodePoolState(newPeersWoInvalidPeers, invalidPeers -- validPeers.filter(_.weight < 3))
  }

  override def toString: String = {
    val validPeersStr = openApiPeers.headOption
      .map(_ => s"Valid peers : ${openApiPeers.mkString(", ")}")
      .getOrElse("No valid peers available")
    val invalidPeersStr =
      invalidPeers.headOption.map(_ => s", invalid peers : ${invalidPeers.mkString(", ")}").getOrElse("")
    s"$validPeersStr$invalidPeersStr"
  }

}

object NodePoolState {
  def empty: NodePoolState = NodePoolState(TreeSet.empty, TreeSet.empty)
}
