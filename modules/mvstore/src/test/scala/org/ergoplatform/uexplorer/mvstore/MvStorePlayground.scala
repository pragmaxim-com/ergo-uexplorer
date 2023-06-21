package org.ergoplatform.uexplorer.mvstore

import org.h2.mvstore.MVStore
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class MvStorePlayground extends AsyncFreeSpec with Matchers {

  "blabla" in {

    val store =
      new MVStore.Builder()
        .fileName(null)
        .cacheSize(512)
        .autoCommitDisabled()
        .open()
    val map = store.openMap[Int, Map[Int, Map[String, String]]]("data")

    println(store.getCurrentVersion)
    store.close()
    0 shouldBe 0
  }
}
