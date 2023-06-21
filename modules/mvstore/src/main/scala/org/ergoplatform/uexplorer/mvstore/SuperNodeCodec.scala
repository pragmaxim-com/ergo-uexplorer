package org.ergoplatform.uexplorer.mvstore

import org.ergoplatform.uexplorer.mvstore
import org.h2.mvstore.MVMap

import java.util
import java.util.Map.Entry
import java.util.stream.Collectors

trait SuperNodeCodec[C[_, _], K, V] {

  def read(sk: K, m: MVMap[K, V]): Option[V]

  def write(to: MVMap[K, V], sk: K, value: V): Appended

  def readPartially(m: MVMap[K, V], only: IterableOnce[K]): C[K, V]

  def readAll(m: MVMap[K, V]): C[K, V]

  def writeAll(to: MVMap[K, V], from: IterableOnce[(K, V)]): Option[(K, V)]

}

object SuperNodeCodec {
  implicit def javaHashMapSuperNodeCodec[K, V]: SuperNodeCodec[java.util.Map, K, V] =
    new SuperNodeCodec[java.util.Map, K, V] {
      override def readAll(from: MVMap[K, V]): util.Map[K, V] =
        from
          .entrySet()
          .stream()
          .collect(Collectors.toMap((e: Entry[K, V]) => e.getKey, (e: Entry[K, V]) => e.getValue))

      override def readPartially(from: MVMap[K, V], only: IterableOnce[K]): util.Map[K, V] =
        mvstore.javaMapOf(only.iterator.flatMap(k => read(k, from).map(k -> _))) // too many instance allocations :-/

      override def writeAll(to: MVMap[K, V], from: IterableOnce[(K, V)]): Option[(K, V)] =
        from.iterator.find(v => !write(to, v._1, v._2))

      override def read(sk: K, m: MVMap[K, V]): Option[V] = Option(m.get(sk))

      override def write(to: MVMap[K, V], sk: K, value: V): Appended = to.put(sk, value) == null
    }
}
