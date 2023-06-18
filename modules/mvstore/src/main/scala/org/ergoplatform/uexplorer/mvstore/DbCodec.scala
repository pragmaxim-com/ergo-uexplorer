package org.ergoplatform.uexplorer.mvstore

trait DbCodec[T] {

  def readAll(bytes: Array[Byte]): T
  def writeAll(obj: T): Array[Byte]

}
