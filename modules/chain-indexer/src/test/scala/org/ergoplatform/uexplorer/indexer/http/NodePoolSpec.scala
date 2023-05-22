package org.ergoplatform.uexplorer.indexer.http

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import sttp.client3._

import scala.collection.immutable.TreeSet
import org.ergoplatform.uexplorer.http.NodePoolState
import org.ergoplatform.uexplorer.http.RemotePeer
import org.ergoplatform.uexplorer.http.LocalNode
import org.ergoplatform.uexplorer.http.RemoteNode

class NodePoolSpec extends AnyFreeSpec with Matchers {

  private val remoteNode = RemoteNode(uri"http://master", "4.0.42", "utxo", 1)
  private val localNode  = LocalNode(uri"http://localhost", "4.0.42", "utxo", 1)
  private val remotePeer = RemotePeer(uri"http://peer", "4.0.42", "utxo", 1)

  "update should remove master/local nodes from invalid" in {
    val initialState  = NodePoolState(TreeSet.empty, TreeSet(localNode, remoteNode))
    val expectedState = NodePoolState(TreeSet(localNode, remoteNode), TreeSet.empty)
    initialState.updatePeers(TreeSet(localNode, remoteNode)) shouldBe expectedState
  }

  "update should not remove peers from invalid" in {
    val initialState  = NodePoolState(TreeSet.empty, TreeSet(remotePeer))
    val expectedState = NodePoolState(TreeSet.empty, TreeSet(remotePeer))
    initialState.updatePeers(TreeSet(remotePeer)) shouldBe expectedState
  }
}
