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
import scala.util.{Random, Try}

class SuperNodeCollector[K: KeyCodec](superNodeFile: File) extends LazyLogging {
  private val keyCodec: KeyCodec[K] = implicitly[KeyCodec[K]]
  private val superNodeFileNameGz   = "supernode-keys.csv.gz"

  private lazy val superNodeKeysWithName: Map[K, String] =
    Source
      .fromInputStream(
        new GZIPInputStream(
          new BufferedInputStream(
            Thread
              .currentThread()
              .getContextClassLoader
              .getResourceAsStream(superNodeFileNameGz)
          )
        )
      )
      .getLines()
      .map(_.trim)
      .filterNot(_.isEmpty)
      .toSet
      .map(k => keyCodec.deserialize(k) -> k)
      .toMap

  def getExistingSuperNodeKeysWithName(storeMapNames: Set[String]): Map[K, String] = {
    val existingSuperNodeNamesByKey = superNodeKeysWithName.filter(e => storeMapNames.contains(e._2))
    logger.info(
      s"MvStore contains ${existingSuperNodeNamesByKey.size} SuperNodes from ${superNodeKeysWithName.size} registered"
    )
    existingSuperNodeNamesByKey
  }

  def getSuperNodeNameByKey(k: K): Option[String] = superNodeKeysWithName.get(k)

  def report(iterator: Iterator[(K, Counter)]): Try[String] = {
    val keysByCount =
      iterator
        .collect {
          case (k, counter) if counter.isHot && !superNodeKeysWithName.contains(k) =>
            keyCodec.serialize(k) -> counter
        }
        .toVector
        .sortBy(_._2.writeOps)(Ordering[Long].reverse)
        .map { case (key, Counter(writeOps, readOps, boxesAdded, boxesRemoved)) =>
          val stats  = s"$writeOps $readOps $boxesAdded $boxesRemoved"
          val indent = 48
          s"$stats ${List.fill(Math.max(1, indent - stats.length))(" ").mkString("")} $key"
        }
    logger.info(s"Collected ${keysByCount.size} supernodes, writing to ${superNodeFile.getAbsolutePath} : ")
    Try {
      keysByCount.headOption
        .map { _ =>
          logger.info(s"Writing supernode keys to ${superNodeFile.getAbsolutePath}")
          val report     = keysByCount.mkString("", "\n", "\n")
          val fileWriter = new FileWriter(superNodeFile)
          try fileWriter.write(report)
          finally fileWriter.close()
          report
        }
        .getOrElse("")
    }
  }
}

object SuperNodeCollector {
  private val hotLimit = 5000
  case class Counter(writeOps: Long, readOps: Long, boxesAdded: Int, boxesRemoved: Int) {
    def this() = this(0, 0, 0, 0)
    def isHot: Boolean = writeOps > hotLimit || readOps > hotLimit || boxesAdded > hotLimit || boxesRemoved > hotLimit
  }

  val randomNumberPerRun: String = Random.alphanumeric.filter(_.isDigit).take(5).mkString
}
