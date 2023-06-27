package org.ergoplatform.uexplorer.mvstore.multiset

import scala.util.Try

trait SuperNodeSetLike[K, C[_], V] {

  def putAllNewOrFail(hotKey: K, values: IterableOnce[V], size: Int): Option[Try[Unit]]

  def removeAllOrFail(hotKey: K, values: IterableOnce[V], size: Int): Option[Try[Unit]]

  def isEmpty: Boolean

  def size: Int

  def totalSize: Int

}
