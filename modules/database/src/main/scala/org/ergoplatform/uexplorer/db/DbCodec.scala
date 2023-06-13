package org.ergoplatform.uexplorer.db

trait DbCodec[T] {

  def read(bytes: Array[Byte]): T
  def write(obj: T): Array[Byte]

}
