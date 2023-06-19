package org.ergoplatform.uexplorer.mvstore

import org.h2.mvstore.MVMap

import java.util
import java.util.Map.Entry
import java.util.stream.Collectors

trait MultiMapCodec[SV[_, _], K, V] extends ValueCodec[SV[K, V]] {

  def readOne(key: K, map: SV[K, V]): Option[V]

  def readAll(bytes: Array[Byte]): SV[K, V]

  def writeAll(map: SV[K, V]): Array[Byte]

  def append(newValueByBoxId: IterableOnce[(K, V)])(
    existingOpt: Option[SV[K, V]]
  ): (Appended, SV[K, V])
}
