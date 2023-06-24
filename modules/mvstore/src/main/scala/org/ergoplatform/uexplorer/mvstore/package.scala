package org.ergoplatform.uexplorer

import org.ergoplatform.uexplorer.mvstore.tempDir

import java.nio.file.Paths
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

package object mvstore {

  val tempDir                    = Paths.get(System.getProperty("java.io.tmpdir"))
  val ergoHomeDir                = Paths.get(System.getProperty("user.home"), ".ergo-uexplorer")
  val randomNumberPerRun: String = Random.alphanumeric.filter(_.isDigit).take(5).mkString

  type CacheSize         = Int
  type HeightCompactRate = Int
  type MaxCompactTime    = FiniteDuration
  type MultiMapId        = String

  type Appended = Boolean // put & not replaced
  type Updated  = Boolean // put & replaced
  type Removed  = Boolean // removed existing
  type Replaced = Boolean // replaced given value

  def javaSetOf[T](e: T): java.util.Set[T] = {
    val set = new java.util.HashSet[T]()
    set.add(e)
    set
  }

  def javaMapOf[K, V](k: K, v: V): java.util.Map[K, V] = {
    val map = new java.util.HashMap[K, V]()
    map.put(k, v)
    map
  }

  def javaMapOf[K, V](kvs: IterableOnce[(K, V)]): java.util.Map[K, V] = {
    val map = new java.util.HashMap[K, V]()
    kvs.iterator.foreach { case (k, v) =>
      map.put(k, v)
    }
    map
  }
}
