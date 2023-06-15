package org.ergoplatform.uexplorer.mvstore

import org.ergoplatform.uexplorer.{Address, BoxId, Height, Value}
import org.h2.mvstore.MVStore
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.ergoplatform.uexplorer.Const.Genesis.Emission
case class Shit(field: String)

class MvStorePlayground extends AsyncFreeSpec with Matchers {

  "blabla" in {

    val store =
      new MVStore.Builder()
        .fileName("/tmp/troloo4")
        .cacheSize(512)
        .autoCommitDisabled()
        .open()
    val map = store.openMap[Int, Map[Height, Map[BoxId, (Address, Value)]]]("data")

    /*
    println(map.get(5))
    println(map.get(6))
    println(map.get(11))
    println(map.get(88))
     */
    // add some data
    map.put(5, Map(1 -> Map(Emission.inputBox -> (Emission.address, 1))))
    map.put(6, Map(1 -> Map(Emission.inputBox -> (Emission.address, 1))))
    assert(map.put(33, Map(1 -> Map(Emission.inputBox -> (Emission.address, 1)))) != null)
    assert(map.put(44, Map(2 -> Map(Emission.inputBox -> (Emission.address, 1)))) != null)
    println(store.getCurrentVersion)
    store.close()
    0 shouldBe 0
  }
}
