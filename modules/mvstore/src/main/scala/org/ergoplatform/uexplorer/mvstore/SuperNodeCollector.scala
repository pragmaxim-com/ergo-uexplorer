package org.ergoplatform.uexplorer.mvstore

import java.io.BufferedInputStream
import java.util.zip.GZIPInputStream
import scala.io.Source
import scala.jdk.CollectionConverters.*

class SuperNodeCollector[HK: HotKeyCodec](id: String) {
  private val hotKeyCodec: HotKeyCodec[HK] = implicitly[HotKeyCodec[HK]]

  private val stringifiedHotKeys: Map[HK, String] =
    Option(
      Thread
        .currentThread()
        .getContextClassLoader
        .getResourceAsStream(s"hot-keys-$id.csv.gz")
    ).map { resource =>
      Source
        .fromInputStream(new GZIPInputStream(new BufferedInputStream(resource)))
        .getLines()
        .map(_.trim)
        .filterNot(_.isEmpty)
        .toSet
        .map(k => hotKeyCodec.deserialize(k) -> k)
        .toMap
    }.getOrElse(Map.empty)

  def getExistingStringifiedHotKeys(mvStoreMapNames: Set[String]): Map[HK, String] =
    stringifiedHotKeys.filter(e => mvStoreMapNames.contains(e._2))

  def getHotKeyString(hotKey: HK): Option[String] = stringifiedHotKeys.get(hotKey)

  def filterAndSortHotKeys(hotKeysWithCounter: Iterator[(HK, SuperNodeCounter)]): Vector[(String, SuperNodeCounter)] =
    hotKeysWithCounter
      .collect {
        case (hotKey, counter) if counter.isHot && !stringifiedHotKeys.contains(hotKey) =>
          hotKeyCodec.serialize(hotKey) -> counter
      }
      .toVector
      .sortBy(_._2.writeOps)(Ordering[Long].reverse)

}

case class SuperNodeCounter(writeOps: Long, readOps: Long, boxesAdded: Int, boxesRemoved: Int) {
  import SuperNodeCounter.hotLimit
  def this() = this(0, 0, 0, 0)

  def isHot: Boolean =
    writeOps > hotLimit || readOps > hotLimit || boxesAdded > hotLimit || boxesRemoved > hotLimit
}

object SuperNodeCounter {
  private val hotLimit = 5000
}
