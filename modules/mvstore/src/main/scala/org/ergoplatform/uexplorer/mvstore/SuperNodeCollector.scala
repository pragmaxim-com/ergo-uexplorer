package org.ergoplatform.uexplorer.mvstore

import org.ergoplatform.uexplorer.mvstore.SuperNodeCounter.{ExistingHotKey, HotKey, NewHotKey}

import java.io.{BufferedInputStream, File, FileInputStream}
import java.nio.file.Path
import java.util.zip.GZIPInputStream
import scala.io.Source
import scala.jdk.CollectionConverters.*
import zio.stream.{ZPipeline, ZSink, ZStream}
import zio.{Task, ZIO}

class SuperNodeCollector[HK: HotKeyCodec](stringifiedHotKeys: Map[HK, String]) {
  private val hotKeyCodec: HotKeyCodec[HK] = implicitly[HotKeyCodec[HK]]

  def getExistingStringifiedHotKeys(mvStoreMapNames: Set[String]): Map[HK, String] =
    stringifiedHotKeys.filter(e => mvStoreMapNames.contains(e._2))

  def getHotKeyString(hotKey: HK): Option[String] = stringifiedHotKeys.get(hotKey)

  def filterAndSortHotKeys(hotKeysWithCounter: Iterator[(HK, SuperNodeCounter)]): Vector[HotKey] =
    val newHotKeys = hotKeysWithCounter
      .collect {
        case (hotKey, counter) if counter.isHot && !stringifiedHotKeys.contains(hotKey) =>
          NewHotKey(hotKeyCodec.serialize(hotKey), counter)
      }
      .toVector
      .sortBy(_._2.writeOps)(Ordering[Long].reverse)

    newHotKeys ++ stringifiedHotKeys.values.map(ExistingHotKey.apply)

}

object SuperNodeCollector {
  def apply[HK: HotKeyCodec](hotKeyPath: Path): Task[SuperNodeCollector[HK]] = ZIO.attempt {
    val hotKeyCodec: HotKeyCodec[HK] = implicitly[HotKeyCodec[HK]]
    new SuperNodeCollector(
      Option(hotKeyPath)
        .filter(_.toFile.exists())
        .map(path => new FileInputStream(path.toFile))
        .orElse(
          Option(
            Thread
              .currentThread()
              .getContextClassLoader
              .getResourceAsStream(hotKeyPath.toFile.getName)
          )
        )
        .map { inputStream =>
          Source
            .fromInputStream(new GZIPInputStream(new BufferedInputStream(inputStream)))
            .getLines()
            .map(_.trim)
            .filterNot(_.isEmpty)
            .toSet
            .map(k => hotKeyCodec.deserialize(k) -> k)
            .toMap
        }
        .getOrElse(Map.empty)
    )
  }
}
