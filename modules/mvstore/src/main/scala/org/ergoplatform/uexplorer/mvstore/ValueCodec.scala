package org.ergoplatform.uexplorer.mvstore

trait ValueCodec[V] {

  def readAll(bytes: Array[Byte]): V
  def writeAll(obj: V): Array[Byte]

}
