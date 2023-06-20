package org.ergoplatform.uexplorer.mvstore

import org.ergoplatform.uexplorer.mvstore.MultiMapLike.MultiMapSize

import scala.util.Try

trait MultiMapLike[PK, C[_, _], K, V] {

  def getFinalReport: Try[String]

  def get(pk: PK, sk: K): Option[V]

  def getAll(pk: PK): Option[C[K, V]]

  def remove(pk: PK): Boolean

  def removeOrFail(pk: PK): Try[C[K, V]]

  def removeAllOrFail(pk: PK, secondaryKeys: IterableOnce[K], size: Int)(f: C[K, V] => Option[C[K, V]]): Try[Unit]

  def isEmpty: Boolean

  def size: MultiMapSize

  def adjustAndForget(pk: PK, entries: IterableOnce[(K, V)], size: Int): Try[_]
}

object MultiMapLike {
  case class MultiMapSize(superNodeSize: Int, superNodeTotalSize: Int, commonSize: Int)
}
