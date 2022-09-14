package org.ergoplatform.uexplorer.indexer.http

import org.ergoplatform.uexplorer.indexer.http.NodePool.NodePoolState
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import sttp.client3._

class NodePoolSpec extends AnyFreeSpec with Matchers {

  private val master = uri"http://master"
  private val local  = uri"http://localhost"
  private val peer   = uri"http://peer"

  "NodePool should" - {

    "prevent master from being invalidated" in {
      val initialState  = NodePoolState(Set(peer, master), Set.empty, master, local)
      val expectedState = NodePoolState(Set(master), Set.empty, master, local)
      initialState.updatePeers(Set.empty, Set(master)) shouldBe expectedState
    }
    "allow local from being invalidated" in {
      val initialState  = NodePoolState(Set(local, master), Set.empty, master, local)
      val expectedState = NodePoolState(Set(master), Set(local), master, local)
      initialState.updatePeers(Set.empty, Set(local)) shouldBe expectedState
    }
    "persist invalid peers" in {
      val initialState  = NodePoolState(Set.empty, Set(peer), master, local)
      val expectedState = NodePoolState(Set(master), Set(peer), master, local)
      initialState.updatePeers(Set.empty, Set.empty) shouldBe expectedState
    }
    "persist invalid local node" in {
      val initialState  = NodePoolState(Set.empty, Set(local), master, local)
      val expectedState = NodePoolState(Set(master), Set(local), master, local)
      initialState.updatePeers(Set.empty, Set.empty) shouldBe expectedState
    }
    "not persist valid peers" in {
      val initialState  = NodePoolState(Set(peer), Set.empty, master, local)
      val expectedState = NodePoolState(Set(master), Set.empty, master, local)
      initialState.updatePeers(Set.empty, Set.empty) shouldBe expectedState
    }
    "not persist local node" in {
      val initialState  = NodePoolState(Set(local), Set.empty, master, local)
      val expectedState = NodePoolState(Set(master), Set.empty, master, local)
      initialState.updatePeers(Set.empty, Set.empty) shouldBe expectedState
    }
    "not put new valid peer over existing invalid" in {
      val initialState  = NodePoolState(Set.empty, Set(peer), master, local)
      val expectedState = NodePoolState(Set(master), Set(peer), master, local)
      initialState.updatePeers(Set(peer), Set.empty) shouldBe expectedState
    }
    "put new valid local node over existing invalid" in {
      val initialState  = NodePoolState(Set.empty, Set(local), master, local)
      val expectedState = NodePoolState(Set(local, master), Set.empty, master, local)
      initialState.updatePeers(Set(local), Set.empty) shouldBe expectedState
    }
  }

}
