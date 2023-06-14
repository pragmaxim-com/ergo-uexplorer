package org.ergoplatform.uexplorer.mvstore

trait DbCodec[T] {

  def read(bytes: Array[Byte]): T
  def write(obj: T): Array[Byte]

}
