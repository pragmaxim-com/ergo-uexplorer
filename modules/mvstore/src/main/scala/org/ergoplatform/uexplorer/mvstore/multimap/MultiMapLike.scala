package org.ergoplatform.uexplorer.mvstore.multimap

import org.ergoplatform.uexplorer.mvstore.MultiColSize

import scala.util.Try

trait MultiMapLike[PK, C[_, _], K, V] {

  def get(pk: PK, sk: K): Option[V]

  def getPartially(pk: PK, sk: IterableOnce[K]): Option[C[K, V]]

  def getAll(pk: PK): Option[C[K, V]]

  def remove(pk: PK): Boolean

  def removeOrFail(pk: PK): Try[C[K, V]]

  def removeAllOrFail(pk: PK, secondaryKeys: IterableOnce[K], size: Int)(f: C[K, V] => Option[C[K, V]]): Try[Unit]

  def isEmpty: Boolean

  def multiSize: MultiColSize

  def adjustAndForget(pk: PK, entries: IterableOnce[(K, V)], size: Int): Try[_]
}
