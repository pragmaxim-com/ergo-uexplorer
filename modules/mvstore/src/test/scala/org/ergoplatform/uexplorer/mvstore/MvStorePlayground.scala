package org.ergoplatform.uexplorer.mvstore

import org.h2.mvstore.MVStore
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.*

class MvStorePlayground extends AsyncFreeSpec with Matchers {

  "blabla" in {

    val store =
      new MVStore.Builder()
        .fileName(null)
        .cacheSize(512)
        .autoCommitDisabled()
        .open()
    val map = store.openMap[Long, Long]("data")

    (0L until 10000L).foreach { i =>
      map.put(i, i)
    }
    map.keyIterator(0).asScala.foreach { key =>
      map.get(key) shouldBe map.getKeyIndex(key)
    }

    map.getKey(5000L) shouldBe 5000L
    map.getKeyIndex(5000L) shouldBe 5000L
    map.remove(5000L)
    map.getKeyIndex(5000L) shouldBe -5001L
    map.getKey(5000L) shouldBe 5001L
    // store.compactFile(1000)

    store.close()

    0 shouldBe 0
  }
}
