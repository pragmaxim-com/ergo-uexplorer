package org.ergoplatform.uexplorer.http

import sttp.client3._
import zio.*
import zio.test.*
import zio.test.Assertion.*

import scala.collection.immutable.TreeSet
import org.ergoplatform.uexplorer.http.NodePoolState
import org.ergoplatform.uexplorer.http.RemotePeer
import org.ergoplatform.uexplorer.http.LocalNode
import org.ergoplatform.uexplorer.http.RemoteNode

object NodePoolSpec extends ZIOSpecDefault {

  private val remoteNode = RemoteNode(uri"http://master", "4.0.42", "utxo", 1)
  private val localNode  = LocalNode(uri"http://localhost", "4.0.42", "utxo", 1)
  private val remotePeer = RemotePeer(uri"http://peer", "4.0.42", "utxo", 1)

  def spec =
    suite("nodepool")(
      test("update should remove master/local nodes from invalid") {
        val initialState  = NodePoolState(TreeSet.empty, TreeSet(localNode, remoteNode))
        val expectedState = NodePoolState(TreeSet(localNode, remoteNode), TreeSet.empty)
        assertTrue(initialState.updatePeers(TreeSet(localNode, remoteNode))._2 == expectedState)
      },
      test("update should not remove peers from invalid") {
        val initialState  = NodePoolState(TreeSet.empty, TreeSet(remotePeer))
        val expectedState = NodePoolState(TreeSet.empty, TreeSet(remotePeer))
        assertTrue(initialState.updatePeers(TreeSet(remotePeer))._2 == expectedState)
      }
    )
}
