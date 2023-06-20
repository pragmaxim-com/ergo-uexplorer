package org.ergoplatform.uexplorer.mvstore

import scala.util.Try

trait SuperNodeMapLike[HK, C[_, _], K, V] {

  def get(hotKey: HK, sk: K): Option[V]

  def getAll(hotKey: HK): Option[C[K, V]]

  def putOnlyNew(hotKey: HK, sk: K, value: V): Option[Boolean]

  def putAllNewOrFail(hotKey: HK, entries: IterableOnce[(K, V)], size: Int): Option[Try[Unit]]

  def remove(hotKey: HK): Option[C[K, V]]

  def removeAllOrFail(hotKey: HK, values: IterableOnce[K], size: Int): Option[Try[Unit]]

  def isEmpty: Boolean

  def size: Int

  def totalSize: Int

}
