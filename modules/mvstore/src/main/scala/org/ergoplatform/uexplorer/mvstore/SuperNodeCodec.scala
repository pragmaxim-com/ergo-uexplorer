package org.ergoplatform.uexplorer.mvstore

import org.h2.mvstore.MVMap

import java.util
import java.util.Map.Entry
import java.util.stream.Collectors

trait SuperNodeCodec[C[_, _], K, V] {

  def read(sk: K, m: MVMap[K, V]): Option[V]

  def write(to: MVMap[K, V], sk: K, value: V): Appended

  def readAll(m: MVMap[K, V]): C[K, V]

  def writeAll(to: MVMap[K, V], from: IterableOnce[(K, V)]): Option[(K, V)]

}

object SuperNodeCodec {
  implicit def javaHashMapSuperNodeCodec[SK, SV]: SuperNodeCodec[java.util.Map, SK, SV] =
    new SuperNodeCodec[java.util.Map, SK, SV] {
      override def readAll(m: MVMap[SK, SV]): util.Map[SK, SV] =
        m
          .entrySet()
          .stream()
          .collect(Collectors.toMap((e: Entry[SK, SV]) => e.getKey, (e: Entry[SK, SV]) => e.getValue))

      override def writeAll(to: MVMap[SK, SV], from: IterableOnce[(SK, SV)]): Option[(SK, SV)] =
        from.iterator.find(v => !write(to, v._1, v._2))

      override def read(sk: SK, m: MVMap[SK, SV]): Option[SV] = Option(m.get(sk))

      override def write(to: MVMap[SK, SV], sk: SK, value: SV): Appended = to.put(sk, value) == null
    }
}
