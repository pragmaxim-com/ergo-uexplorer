package org.ergoplatform.uexplorer.mvstore

import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.mvstore.SuperNodeCollector.Counter
import org.h2.mvstore.MVStore

import java.io.{BufferedInputStream, File, FileInputStream, FileWriter}
import java.nio.file.{Path, Paths}
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.GZIPInputStream
import scala.collection.concurrent
import scala.io.Source
import scala.jdk.CollectionConverters.*
import scala.util.{Random, Success, Try}

class SuperNodeCollector[HK: HotKeyCodec](id: String) extends LazyLogging {
  private val hotKeyCodec: HotKeyCodec[HK] = implicitly[HotKeyCodec[HK]]

  private lazy val stringifiedHotKeys: Map[HK, String] =
    Source
      .fromInputStream(
        new GZIPInputStream(
          new BufferedInputStream(
            Thread
              .currentThread()
              .getContextClassLoader
              .getResourceAsStream(s"hot-keys-$id.csv.gz")
          )
        )
      )
      .getLines()
      .map(_.trim)
      .filterNot(_.isEmpty)
      .toSet
      .map(k => hotKeyCodec.deserialize(k) -> k)
      .toMap

  def getExistingStringifiedHotKeys(mvStoreMapNames: Set[String]): Map[HK, String] = {
    val existingStringifiedHotKeys = stringifiedHotKeys.filter(e => mvStoreMapNames.contains(e._2))
    logger.info(
      s"MvStore contains ${existingStringifiedHotKeys.size} SuperNodes from ${stringifiedHotKeys.size} registered"
    )
    existingStringifiedHotKeys
  }

  def getHotKeyString(hotKey: HK): Option[String] = stringifiedHotKeys.get(hotKey)

  def filterAndSortHotKeys(hotKeysWithCounter: Iterator[(HK, Counter)]): Vector[(String, Counter)] =
    hotKeysWithCounter
      .collect {
        case (hotKey, counter) if counter.isHot && !stringifiedHotKeys.contains(hotKey) =>
          hotKeyCodec.serialize(hotKey) -> counter
      }
      .toVector
      .sortBy(_._2.writeOps)(Ordering[Long].reverse)

}

object SuperNodeCollector {
  private val hotLimit = 10000
  case class Counter(writeOps: Long, readOps: Long, boxesAdded: Int, boxesRemoved: Int) {
    def this() = this(0, 0, 0, 0)
    def isHot: Boolean = writeOps > hotLimit || readOps > hotLimit || boxesAdded > hotLimit || boxesRemoved > hotLimit
  }
}
