package org.ergoplatform.uexplorer.indexer.http

import org.ergoplatform.uexplorer.indexer.http.NodePool.NodePoolState
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import sttp.client3._

class NodePoolSpec extends AnyFreeSpec with Matchers {

  private val masterUri  = uri"http://master"
  private val validUri   = uri"http://valid"
  private val invalidUri = uri"http://invalid"

  "NodePool should" - {

    "prevent master from getting removed" in {
      val state = NodePoolState(Set(validUri, masterUri), Set.empty, masterUri)
      state.updatePeers(Set.empty, Set(masterUri)) shouldBe NodePoolState(Set(masterUri), Set.empty, masterUri)
    }
    "persist invalid peers" in {
      val state = NodePoolState(Set.empty, Set(invalidUri), masterUri)
      state.updatePeers(Set.empty, Set.empty) shouldBe NodePoolState(Set(masterUri), Set(invalidUri), masterUri)
    }
    "not persist valid peers" in {
      val state = NodePoolState(Set(validUri), Set(invalidUri), masterUri)
      state.updatePeers(Set.empty, Set.empty) shouldBe NodePoolState(Set(masterUri), Set(invalidUri), masterUri)
    }
    "not put new valid over existing invalid" in {
      val state = NodePoolState(Set(validUri), Set(invalidUri), masterUri)
      state.updatePeers(Set(invalidUri), Set.empty) shouldBe NodePoolState(Set(masterUri), Set(invalidUri), masterUri)
    }
  }

}
