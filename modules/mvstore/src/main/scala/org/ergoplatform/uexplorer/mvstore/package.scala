package org.ergoplatform.uexplorer

import java.nio.file.Paths

package object mvstore {

  val tempDir     = Paths.get(System.getProperty("java.io.tmpdir"))
  val ergoHomeDir = Paths.get(System.getProperty("user.home"), ".ergo-uexplorer")

  type CacheSize         = Int
  type HeightCompactRate = Int
  type MaxCompactTime    = zio.Duration
  type MultiColId        = String

  type Appended = Boolean // put & not replaced
  type Updated  = Boolean // put & replaced
  type Removed  = Boolean // removed existing
  type Replaced = Boolean // replaced given value

  def javaSetOf[V](e: V): java.util.Set[V] = {
    val set = new java.util.HashSet[V]()
    set.add(e)
    set
  }

  def javaSetOf[V](values: IterableOnce[V]): java.util.Set[V] = {
    val set = new java.util.HashSet[V]()
    values.iterator.foreach { value =>
      set.add(value)
    }
    set
  }

  def javaMapOf[K, V](k: K, v: V): java.util.Map[K, V] = {
    val map = new java.util.HashMap[K, V]()
    map.put(k, v)
    map
  }

  def javaMapOf[K, V](kvs: IterableOnce[(K, V)]): java.util.Map[K, V] = {
    val map = new java.util.HashMap[K, V]()
    kvs.iterator.foreach { case (k, v) =>
      map.put(k, v)
    }
    map
  }
}
