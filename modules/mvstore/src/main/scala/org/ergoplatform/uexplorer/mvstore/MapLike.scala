package org.ergoplatform.uexplorer.mvstore

import zio.Task

import scala.util.Try

trait MapLike[K, V] {

  def get(key: K): Option[V]

  def getWithOp(key: K)(f: Option[V] => Option[V]): Option[V]

  def isEmpty: Boolean

  def size: Int

  def remove(key: K): Option[V]

  def removeAndForget(key: K): Removed

  def removeAndForgetOrFail(key: K): Try[Unit]

  def removeOrFail(key: K): Try[V]

  def removeAllOrFail(keys: IterableOnce[K]): Try[Unit]

  def removeAllExisting(keys: IterableOnce[K]): Unit

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

  def putAndForget(key: K, value: V): Appended

  def putIfAbsent(key: K, value: V): Option[V]

  def putIfAbsentOrFail(key: K, value: V): Task[Unit]

  def putAllNewOrFail(entries: IterableOnce[(K, V)]): Try[Unit]

  def putIfAbsentAndForget(key: K, value: V): Unit

  def replace(key: K, value: V): Option[V]

  def replace(key: K, oldValue: V, newValue: V): Replaced

  def removeOrUpdate(k: K)(f: V => Option[V]): Option[V]

  def removeOrUpdateOrFail(k: K)(f: V => Option[V]): Try[Unit]

  def adjust(k: K)(f: Option[V] => V): V

  def adjustCollection(k: K)(f: Option[V] => (Appended, V)): (Appended, V)
}
