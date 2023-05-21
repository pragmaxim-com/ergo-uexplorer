package org.ergoplatform.uexplorer.indexer

import org.ergoplatform.uexplorer.{Address, BoxId, Height, Value}
import org.h2.mvstore.MVStore
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.ergoplatform.uexplorer.Const.Genesis.Emission
case class Shit(field: String)

class MvStorePlayground extends AsyncFreeSpec with Matchers {

  "blabla" in {

    val store = MVStore.open("/tmp/meeh3");
    val map   = store.openMap[Int, Map[Height, Map[BoxId, (Address, Value)]]]("data")

    // add some data
    map.put(1, Map(1 -> Map(Emission.box -> (Emission.address, 1))))
    map.put(2, Map(2 -> Map(Emission.box -> (Emission.address, 1))))

    // get the current version, for later use
    val oldVersion = store.getCurrentVersion

    // from now on, the old version is read-only
    store.commit

    // more changes, in the new version
    // changes can be rolled back if required
    // changes always go into "head" (the newest version)
    map.put(1, Map(11 -> Map(Emission.box -> (Emission.address, 11))))
    map.remove(2)

    // access the old data (before the commit)
    val oldMap = map.openVersion(oldVersion)

    // print the old version (can be done
    // concurrently with further modifications)
    // this will print "Hello" and "World":
    System.out.println(oldMap.get(1).head)
    System.out.println(oldMap.get(2).head)

    // print the newest version ("Hi")
    System.out.println(map.get(1).head)
    0 shouldBe 0
  }
}
