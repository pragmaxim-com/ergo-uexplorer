package org.ergoplatform.uexplorer.mvstore

import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.mvstore.SuperNodeCollector.{randomNumberPerRun, Counter}
import org.h2.mvstore.MVStore

import java.io.{BufferedInputStream, File, FileInputStream, FileWriter}
import java.nio.file.{Path, Paths}
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.GZIPInputStream
import scala.collection.concurrent
import scala.io.Source
import scala.jdk.CollectionConverters.*
import scala.util.{Random, Success, Try}

class SuperNodeCollector[HK: HotKeyCodec](inputHotKeysFileName: String, outputHotKeysFile: File) extends LazyLogging {
  private val hotKeyCodec: HotKeyCodec[HK] = implicitly[HotKeyCodec[HK]]

  private lazy val stringifiedHotKeys: Map[HK, String] =
    Source
      .fromInputStream(
        new GZIPInputStream(
          new BufferedInputStream(
            Thread
              .currentThread()
              .getContextClassLoader
              .getResourceAsStream(inputHotKeysFileName)
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

  def writeReport(hotKeysWithCounter: Iterator[(HK, Counter)]): Try[_] =
    if (outputHotKeysFile.exists()) {
      logger.info(s"Skipping report as file ${outputHotKeysFile.getAbsolutePath} already exists")
      Success(())
    } else {
      val keysByCount =
        hotKeysWithCounter
          .collect {
            case (hotKey, counter) if counter.isHot && !stringifiedHotKeys.contains(hotKey) =>
              hotKeyCodec.serialize(hotKey) -> counter
          }
          .toVector
          .sortBy(_._2.writeOps)(Ordering[Long].reverse)
          .map { case (hotKeyString, Counter(writeOps, readOps, boxesAdded, boxesRemoved)) =>
            val stats  = s"$writeOps $readOps $boxesAdded $boxesRemoved ${boxesAdded - boxesRemoved}"
            val indent = 45
            s"$stats ${List.fill(Math.max(4, indent - stats.length))(" ").mkString("")} $hotKeyString"
          }
      Try {
        keysByCount.headOption
          .map { _ =>
            logger.info(s"Writing ${keysByCount.size} hot keys to ${outputHotKeysFile.getAbsolutePath}")
            val report     = keysByCount.mkString("", "\n", "\n")
            val fileWriter = new FileWriter(outputHotKeysFile)
            try fileWriter.write(report)
            finally fileWriter.close()
            report
          }
          .getOrElse("")
      }
    }
}

object SuperNodeCollector {
  private val hotLimit = 10000
  case class Counter(writeOps: Long, readOps: Long, boxesAdded: Int, boxesRemoved: Int) {
    def this() = this(0, 0, 0, 0)
    def isHot: Boolean = writeOps > hotLimit || readOps > hotLimit || boxesAdded > hotLimit || boxesRemoved > hotLimit
  }

  val randomNumberPerRun: String = Random.alphanumeric.filter(_.isDigit).take(5).mkString
}
