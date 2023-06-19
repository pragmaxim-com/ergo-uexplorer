package org.ergoplatform.uexplorer

package object mvstore {

  type Appended = Boolean // put & not replaced
  type Updated  = Boolean // put & replaced
  type Removed  = Boolean // removed existing
  type Replaced = Boolean // replaced given value

  def javaSetOf[T](e: T): java.util.Set[T] = {
    val set = new java.util.HashSet[T]()
    set.add(e)
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
