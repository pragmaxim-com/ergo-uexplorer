package org.ergoplatform.uexplorer.indexer.http

import org.ergoplatform.uexplorer.indexer.http.NodePool.NodePoolState
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import sttp.client3._

class NodePoolSpec extends AnyFreeSpec with Matchers {

  private val remoteNode = RemoteNode(uri"http://master", "4.0.42", "utxo", 1)
  private val localNode  = LocalNode(uri"http://localhost", "4.0.42", "utxo", 1)
  private val remotePeer = RemotePeer(uri"http://peer", "4.0.42", "utxo", 1)

  "update should remove master/local nodes from invalid" in {
    val initialState  = NodePoolState(Set.empty, Set(localNode, remoteNode))
    val expectedState = NodePoolState(Set(localNode, remoteNode), Set.empty)
    initialState.updatePeers(Set(localNode, remoteNode)) shouldBe expectedState
  }

  "update should not remove peers from invalid" in {
    val initialState  = NodePoolState(Set.empty, Set(remotePeer))
    val expectedState = NodePoolState(Set.empty, Set(remotePeer))
    initialState.updatePeers(Set(remotePeer)) shouldBe expectedState
  }
}
