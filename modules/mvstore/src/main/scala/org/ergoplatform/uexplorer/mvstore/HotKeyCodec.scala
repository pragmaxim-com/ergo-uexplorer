package org.ergoplatform.uexplorer.mvstore

trait HotKeyCodec[T] {
  def serialize(key: T): String

  def deserialize(key: String): T
}
