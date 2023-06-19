package org.ergoplatform.uexplorer.mvstore

trait KeyCodec[T] {
  def serialize(key: T): String

  def deserialize(key: String): T
}
