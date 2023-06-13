package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.Address
import org.h2.mvstore.MVMap.DecisionMaker
import org.h2.mvstore.{MVMap, MVStore}
import scala.jdk.CollectionConverters.*
import scala.util.Try

class MvUeMap[K, V: DbCodec](name: String, store: MVStore) extends UeMap[K, V] {

  private val underlying: MVMap[K, Array[Byte]] = store.openMap[K, Array[Byte]](name)

  private val codec: DbCodec[V] = implicitly[DbCodec[V]]

  def get(key: K): Option[V] = Option(underlying.get(key)).map(codec.read)

  def isEmpty: Boolean = underlying.isEmpty

  def size: Int = underlying.size()

  def remove(key: K): Option[V] = Option(underlying.remove(key)).map(codec.read)

  def ceilingKey(key: K): Option[K] = Option(underlying.ceilingKey(key))

  def clear(): Try[Unit] = Try(underlying.clear())

  def containsKey(key: K): Boolean = underlying.containsKey(key)

  def iterator(fromK: Option[K], toK: Option[K], reverse: Boolean): Iterator[(K, V)] = new Iterator[(K, V)]() {
    private val cursor = underlying.cursor(fromK.orNull.asInstanceOf[K], toK.orNull.asInstanceOf[K], reverse)

    override def hasNext: Boolean = cursor.hasNext

    override def next(): (K, V) = cursor.next() -> codec.read(cursor.getValue)
  }

  def keyIterator(from: Option[K]): Iterator[K] = underlying.keyIterator(from.orNull.asInstanceOf[K]).asScala

  def keyIteratorReverse(from: Option[K]): Iterator[K] = underlying.keyIteratorReverse(from.orNull.asInstanceOf[K]).asScala

  def firstKey: Option[K] = Option(underlying.firstKey())

  def floorKey(key: K): Option[K] = Option(underlying.floorKey(key))

  def higherKey(key: K): Option[K] = Option(underlying.higherKey(key))

  def lowerKey(key: K): Option[K] = Option(underlying.lowerKey(key))

  def lastKey: Option[K] = Option(underlying.lastKey())

  def keySet: java.util.Set[K] = underlying.keySet()

  def keyList: java.util.List[K] = underlying.keyList()

  def put(key: K, value: V): Option[V] = Option(underlying.put(key, codec.write(value))).map(codec.read)

  def putIfAbsent(key: K, value: V): Option[V] = Option(underlying.putIfAbsent(key, codec.write(value))).map(codec.read)

  def replace(key: K, value: V): Option[V] = Option(underlying.replace(key, codec.write(value))).map(codec.read)

  def replace(key: K, oldValue: V, newValue: V): Boolean =
    underlying.replace(key, codec.write(oldValue), codec.write(newValue))

  def putOrRemove(k: K)(f: Option[V] => Option[V]): Option[V] =
    f(Option(underlying.get(k)).map(codec.read)) match {
      case None =>
        Option(underlying.remove(k)).map(codec.read)
      case Some(v) =>
        Option(underlying.put(k, codec.write(v))).map(codec.read)
    }

  def adjust(k: K)(f: Option[V] => V): Option[V] =
    Option(underlying.put(k, codec.write(f(Option(underlying.get(k)).map(codec.read)))))
      .map(codec.read)

}
