package org.ergoplatform.uexplorer.mvstore.multiset

import scala.util.Try

trait SuperNodeSetLike[K, C[_], V] {

  def putAllNewOrFail(hotKey: K, values: IterableOnce[V], size: Int): Option[Try[Unit]]

  def removeAllOrFail(hotKey: K, values: IterableOnce[V], size: Int): Option[Try[Unit]]

  def get(hotKey: K): Option[C[V]]

  def contains(hotKey: K): Boolean

  def contains(hotKey: K, v: V): Option[Boolean]

  def isEmpty: Boolean

  def size: Int

  def totalSize: Int

}
