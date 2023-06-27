package org.ergoplatform.uexplorer.mvstore.multimap

import org.ergoplatform.uexplorer.mvstore.{Appended, ValueCodec}
import org.h2.mvstore.MVMap

import java.util
import java.util.Map.Entry
import java.util.stream.Collectors

trait MultiMapCodec[C[_, _], K, V] extends ValueCodec[C[K, V]] {

  def readOne(key: K, map: C[K, V]): Option[V]

  def readAll(bytes: Array[Byte]): C[K, V]

  def readPartially(only: IterableOnce[K])(existingOpt: Option[C[K, V]]): Option[C[K, V]]

  def writeAll(map: C[K, V]): Array[Byte]

  def append(newValueByBoxId: IterableOnce[(K, V)])(
    existingOpt: Option[C[K, V]]
  ): (Appended, C[K, V])
}
