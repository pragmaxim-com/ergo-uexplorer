package org.ergoplatform.uexplorer.mvstore

import org.ergoplatform.uexplorer.mvstore.SuperNodeCounter.{ExistingHotKey, HotKey, NewHotKey}

import java.io.{BufferedInputStream, FileInputStream}
import java.nio.file.Path
import java.util.zip.GZIPInputStream
import scala.io.Source
import scala.jdk.CollectionConverters.*
import zio.stream.{ZSink, ZStream}
import zio.{Task, ZIO}

class SuperNodeCollector[HK: HotKeyCodec](id: String) {
  private val hotKeyCodec: HotKeyCodec[HK] = implicitly[HotKeyCodec[HK]]
  private val hotKeyFileName               = s"hot-keys-$id.csv.gz"
  private val stringifiedHotKeys: Map[HK, String] =
    Option(ergoHomeDir.resolve(hotKeyFileName))
      .filter(_.toFile.exists())
      .map(path => new FileInputStream(path.toFile))
      .orElse(
        Option(
          Thread
            .currentThread()
            .getContextClassLoader
            .getResourceAsStream(hotKeyFileName)
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

  def getExistingStringifiedHotKeys(mvStoreMapNames: Set[String]): Map[HK, String] =
    stringifiedHotKeys.filter(e => mvStoreMapNames.contains(e._2))

  def getHotKeyString(hotKey: HK): Option[String] = stringifiedHotKeys.get(hotKey)

  def filterAndSortHotKeys(hotKeysWithCounter: Iterator[(HK, SuperNodeCounter)]): (Path, Vector[HotKey]) =
    val newHotKeys = hotKeysWithCounter
      .collect {
        case (hotKey, counter) if counter.isHot && !stringifiedHotKeys.contains(hotKey) =>
          NewHotKey(hotKeyCodec.serialize(hotKey), counter)
      }
      .toVector
      .sortBy(_._2.writeOps)(Ordering[Long].reverse)

    val existingHotKeys = stringifiedHotKeys.values
    (ergoHomeDir.resolve(hotKeyFileName), newHotKeys ++ existingHotKeys.map(ExistingHotKey.apply))

}

case class SuperNodeCounter(writeOps: Long, readOps: Long, boxesAdded: Int, boxesRemoved: Int) {
  import SuperNodeCounter.hotLimit
  def this() = this(0, 0, 0, 0)

  def isHot: Boolean =
    writeOps > hotLimit || readOps > hotLimit || boxesAdded > hotLimit || boxesRemoved > hotLimit
}

object SuperNodeCounter {
  private val hotLimit = 500

  sealed trait HotKey {
    def key: String
  }
  case class NewHotKey(key: String, counter: SuperNodeCounter) extends HotKey
  case class ExistingHotKey(key: String) extends HotKey

  def writeReport(lines: IndexedSeq[String], targetPath: Path): Task[Unit] =
    ZIO.attempt(targetPath.toFile.delete()) *>
      ZStream
        .fromIterable(lines)
        .intersperse("\n")
        .run(ZSink.fromPath(targetPath).contramapChunks[String](_.flatMap(_.getBytes)))
        .unit

}
