package org.ergoplatform.uexplorer.mvstore.multiset

import org.ergoplatform.uexplorer.mvstore
import org.ergoplatform.uexplorer.mvstore.Appended
import org.h2.mvstore.MVMap
import org.h2.mvstore.db.NullValueDataType
import org.h2.value.{Value, ValueNull}

import java.util
import java.util.Map.Entry
import java.util.stream.Collectors

trait SuperNodeSetCodec[C[_], V] {

  def contains(value: V, m: MVMap[V, Value]): Boolean

  def write(to: MVMap[V, Value], value: V): Appended

  def readAll(m: MVMap[V, Value]): C[V]

  def writeAll(to: MVMap[V, Value], from: IterableOnce[V]): Option[V]

}

object SuperNodeSetCodec {
  implicit def javaHashSetSuperNodeCodec[V]: SuperNodeSetCodec[java.util.Set, V] =
    new SuperNodeSetCodec[java.util.Set, V] {
      override def readAll(from: MVMap[V, Value]): util.Set[V] =
        new java.util.HashSet[V](from.keySet())

      override def writeAll(to: MVMap[V, Value], from: IterableOnce[V]): Option[V] =
        from.iterator.find(v => !write(to, v))

      override def contains(value: V, m: MVMap[V, Value]): Boolean = m.containsKey(value)

      override def write(to: MVMap[V, Value], value: V): Appended =
        to.put(value, ValueNull.INSTANCE) == null
    }
}
