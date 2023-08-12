package org.ergoplatform.uexplorer.mvstore.multiset

import org.ergoplatform.uexplorer.mvstore.MultiColSize

import scala.util.Try

trait MultiSetLike[K, C[_], V] {

  def isEmpty: Boolean

  def get(k: K): Option[C[V]]

  def contains(k: K): Boolean

  def size: MultiColSize

  def removeSubsetOrFail(k: K, values: IterableOnce[V], size: Int)(f: C[V] => Option[C[V]]): Try[Unit]
  def adjustAndForget(k: K, values: IterableOnce[V], size: Int): Try[_]
}
