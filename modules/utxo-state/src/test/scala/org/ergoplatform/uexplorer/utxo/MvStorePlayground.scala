package org.ergoplatform.uexplorer.utxo

import org.ergoplatform.uexplorer.{Address, BoxId, Height, Value}
import org.h2.mvstore.MVStore
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.ergoplatform.uexplorer.Const.Genesis.Emission
case class Shit(field: String)

class MvStorePlayground extends AsyncFreeSpec with Matchers {

  "blabla" in {

    val store = MVStore.open(null);
    val map   = store.openMap[Int, Map[Height, Map[BoxId, (Address, Value)]]]("data")

    map.put(1, Map())
    // get the current version, for later use
    val oldVersion1 = store.commit

    println(s"1 $oldVersion1")

    map.put(1, Map())
    // get the current version, for later use
    val oldVersion2 = store.commit

    println(s"2 $oldVersion2")

    store.setStoreVersion(1)
    println(s"4 ${store.getCurrentVersion}")

    store.close()
    0 shouldBe 0
  }
}
