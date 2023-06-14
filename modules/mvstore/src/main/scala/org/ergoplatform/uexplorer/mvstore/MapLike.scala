package org.ergoplatform.uexplorer.mvstore

import scala.util.Try

trait MapLike[K, V] {

  def get(key: K): Option[V]

  def isEmpty: Boolean

  def size: Int

  def remove(key: K): Option[V]

  def removeAndForget(key: K): Unit
  
  def ceilingKey(key: K): Option[K]

  def clear(): Try[Unit]

  def containsKey(key: K): Boolean

  def iterator(from: Option[K], to: Option[K], reverse: Boolean): Iterator[(K, V)]

  def keyIterator(from: Option[K]): Iterator[K]

  def keyIteratorReverse(from: Option[K]): Iterator[K]

  def firstKey: Option[K]

  def floorKey(key: K): Option[K]

  def higherKey(key: K): Option[K]

  def lowerKey(key: K): Option[K]

  def lastKey: Option[K]

  def keySet: java.util.Set[K]

  def keyList: java.util.List[K]

  def put(key: K, value: V): Option[V]

  def putAndForget(key: K, value: V): Unit
  
  def putIfAbsent(key: K, value: V): Option[V]

  def putIfAbsentAndForget(key: K, value: V): Unit
  
  def replace(key: K, value: V): Option[V]

  def replace(key: K, oldValue: V, newValue: V): Boolean

  def putOrRemove(k: K)(f: Option[V] => Option[V]): Option[V]

  def putOrRemoveAndForget(k: K)(f: Option[V] => Option[V]): Unit
  
  def adjust(k: K)(f: Option[V] => V): Option[V]

  def adjustAndForget(k: K)(f: Option[V] => V): Unit
}
