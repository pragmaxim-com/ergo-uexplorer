package org.ergoplatform.uexplorer.mvstore

import scala.util.Try

trait SuperNodeMapLike[SK, SV[_, _], K, V] {

  def getFinalReport: Try[String]
  
  def get(key: SK, secondaryKey: K): Option[V]

  def getAll(key: SK): Option[SV[K, V]]

  def putOnlyNew(key: SK, secondaryKey: K, value: V): Option[Boolean]

  def putAllNewOrFail(sk: SK, entries: IterableOnce[(K, V)], size: Int): Option[Try[Unit]]

  def remove(sk: SK): Option[SV[K, V]]

  def removeAllOrFail(sk: SK, values: IterableOnce[K], size: Int): Option[Try[Unit]]

  def isEmpty: Boolean

  def size: Int
  
  def totalSize: Int

}
