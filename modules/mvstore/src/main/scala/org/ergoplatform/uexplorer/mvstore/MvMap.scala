package org.ergoplatform.uexplorer.mvstore

import org.ergoplatform.uexplorer.Address
import org.h2.mvstore.MVMap.DecisionMaker
import org.h2.mvstore.{MVMap, MVStore}

import java.util.Map.copyOf
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

class MvMap[K, V: DbCodec](name: String, store: MVStore) extends MapLike[K, V] {

  private val underlying: MVMap[K, Array[Byte]] = store.openMap[K, Array[Byte]](name)

  private val codec: DbCodec[V] = implicitly[DbCodec[V]]

  def get(key: K): Option[V] = Option(underlying.get(key)).map(codec.readAll)

  def isEmpty: Boolean = underlying.isEmpty

  def size: Int = underlying.size()

  def remove(key: K): Option[V] = Option(underlying.remove(key)).map(codec.readAll)

  def removeAndForget(key: K): Boolean = underlying.remove(key) != null

  def removeAndForgetOrFail(key: K): Try[Unit] =
    if (underlying.remove(key) != null)
      Success(())
    else
      Failure(new AssertionError(s"Removing non-existing key $key"))

  def removeOrFail(key: K): Try[V] =
    Try(underlying.remove(key)).flatMap {
      case v if v != null =>
        Failure(new AssertionError(s"Removing non-existing key $key"))
      case v =>
        Success(codec.readAll(v))
    }

  def removeAllOrFail(keys: Iterable[K]): Try[Unit] =
    keys.find(key => underlying.remove(key) == null).fold(Success(())) { key =>
      Failure(new AssertionError(s"Removing non-existing key $key"))
    }

  def ceilingKey(key: K): Option[K] = Option(underlying.ceilingKey(key))

  def clear(): Try[Unit] = Try(underlying.clear())

  def containsKey(key: K): Boolean = underlying.containsKey(key)

  def iterator(fromK: Option[K], toK: Option[K], reverse: Boolean): Iterator[(K, V)] = new Iterator[(K, V)]() {
    private val cursor = underlying.cursor(fromK.orNull.asInstanceOf[K], toK.orNull.asInstanceOf[K], reverse)

    override def hasNext: Boolean = cursor.hasNext

    override def next(): (K, V) = cursor.next() -> codec.readAll(cursor.getValue)
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

  def put(key: K, value: V): Option[V] = Option(underlying.put(key, codec.writeAll(value))).map(codec.readAll)

  def putAndForget(key: K, value: V): Boolean = underlying.put(key, codec.writeAll(value)) == null

  def putAllNewOrFail(entries: IterableOnce[(K, V)]): Try[Unit] =
    entries.iterator
      .find(e => underlying.put(e._1, codec.writeAll(e._2)) != null)
      .map(e => Failure(new AssertionError(s"Key ${e._1} was already present!")))
      .getOrElse(Success(()))

  def putIfAbsent(key: K, value: V): Option[V] = Option(underlying.putIfAbsent(key, codec.writeAll(value))).map(codec.readAll)

  def putIfAbsentOrFail(key: K, value: V): Try[Unit] =
    Option(underlying.putIfAbsent(key, codec.writeAll(value))).fold(Success(())) { oldVal =>
      Failure(new AssertionError(s"Key $key already present with value ${codec.readAll(oldVal)}"))
    }

  def putIfAbsentAndForget(key: K, value: V): Unit = underlying.putIfAbsent(key, codec.writeAll(value))

  def replace(key: K, value: V): Option[V] = Option(underlying.replace(key, codec.writeAll(value))).map(codec.readAll)

  def replace(key: K, oldValue: V, newValue: V): Boolean =
    underlying.replace(key, codec.writeAll(oldValue), codec.writeAll(newValue))

  def removeOrUpdate(k: K)(f: V => Option[V]): Option[V] =
    Option(underlying.get(k)).map(codec.readAll) match {
      case None =>
        None
      case Some(v) =>
        f(v) match {
          case None =>
            Option(underlying.remove(k)).map(codec.readAll)
          case Some(v) =>
            Option(underlying.put(k, codec.writeAll(v))).map(codec.readAll)
        }
    }

  def removeOrUpdateOrFail(k: K)(f: V => Option[V]): Try[Unit] =
    Option(underlying.get(k)).map(codec.readAll) match {
      case None =>
        Failure(new AssertionError(s"Removing or updating non-existing key $k"))
      case Some(v) =>
        f(v) match {
          case None =>
            Try(assert(underlying.remove(k) != null, s"Removing non-existing key $k"))
          case Some(v) =>
            underlying.put(k, codec.writeAll(v))
            Success(())
        }
    }

  def adjustAndForget(k: K)(f: Option[V] => V): Boolean =
    underlying.put(k, codec.writeAll(f(Option(underlying.get(k)).map(codec.readAll)))) == null

}
