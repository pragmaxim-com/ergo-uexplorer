package org.ergoplatform.uexplorer.mvstore

import scala.util.Try

trait MultiMapLike[PK, C[_, _], K, V] {

  def get(key: PK): Option[C[K, V]]

  def remove(key: PK): Boolean

  def removeOrFail(key: PK): Try[C[K, V]]

  def removeAllOrFail(k: PK, secondaryKeys: IterableOnce[K])(f: C[K, V] => Option[C[K, V]]): Try[Unit]

  def isEmpty: Boolean

  def size: Int

  def adjustAndForget(key: PK, entries: IterableOnce[(K, V)])(f: Option[C[K, V]] => C[K, V]): Try[_]
}
