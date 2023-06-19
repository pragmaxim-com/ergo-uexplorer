package org.ergoplatform.uexplorer.mvstore

trait ValueCodec[T] {

  def readAll(bytes: Array[Byte]): T
  def writeAll(obj: T): Array[Byte]

}
