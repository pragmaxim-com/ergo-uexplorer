package org.ergoplatform.uexplorer.mvstore

import org.h2.mvstore.MVMap

import java.util
import java.util.Map.Entry
import java.util.stream.Collectors

trait SuperNodeCodec[SV[_, _], K, V] {

  def read(key: K, m: MVMap[K, V]): Option[V]

  def readAll(m: MVMap[K, V]): SV[K, V]

  def write(to: MVMap[K, V], key: K, value: V): Boolean

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

      override def read(key: SK, m: MVMap[SK, SV]): Option[SV] = Option(m.get(key))

      override def write(to: MVMap[SK, SV], key: SK, value: SV): Boolean = to.put(key, value) == null
    }
}
