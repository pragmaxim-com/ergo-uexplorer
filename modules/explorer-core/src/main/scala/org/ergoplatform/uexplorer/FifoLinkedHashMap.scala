package org.ergoplatform.uexplorer

import java.util

class FifoLinkedHashMap[K, V](maxSize: Int = 100) extends util.LinkedHashMap[K, V] {
  override def removeEldestEntry(eldest: java.util.Map.Entry[K, V]): Boolean = this.size > maxSize
}
