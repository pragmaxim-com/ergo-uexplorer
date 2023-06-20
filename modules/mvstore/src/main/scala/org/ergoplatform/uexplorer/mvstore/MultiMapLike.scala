package org.ergoplatform.uexplorer.mvstore

import org.ergoplatform.uexplorer.mvstore.MultiMapLike.MultiMapSize

import scala.util.Try

trait MultiMapLike[PK, C[_, _], K, V] {

  def getFinalReport: Option[String]

  def get(key: PK, secondaryKey: K): Option[V]

  def getAll(key: PK): Option[C[K, V]]

  def remove(key: PK): Boolean

  def removeOrFail(key: PK): Try[C[K, V]]

  def removeAllOrFail(k: PK, secondaryKeys: IterableOnce[K], size: Int)(f: C[K, V] => Option[C[K, V]]): Try[Unit]

  def isEmpty: Boolean

  def size: MultiMapSize

  def adjustAndForget(key: PK, entries: IterableOnce[(K, V)], size: Int): Try[_]
}

object MultiMapLike {
  case class MultiMapSize(superNodeSize: Int, superNodeTotalSize: Int, commonSize: Int)
}