package org.ergoplatform.uexplorer.mvstore.multiset

import org.ergoplatform.uexplorer.mvstore.{Appended, ValueCodec}
import org.h2.mvstore.MVMap

import java.util
import java.util.Map.Entry
import java.util.stream.Collectors

trait MultiSetCodec[C[_], V] extends ValueCodec[C[V]] {

  def readAll(bytes: Array[Byte]): C[V]

  def writeAll(map: C[V]): Array[Byte]

  def append(xs: IterableOnce[V])(
    existingOpt: Option[C[V]]
  ): (Appended, C[V])
}
