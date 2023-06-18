package org.ergoplatform.uexplorer.storage

import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.Address
import org.ergoplatform.uexplorer.Const.FeeContract

import java.io.{BufferedInputStream, FileWriter}
import java.util.zip.GZIPInputStream
import scala.io.Source
import org.ergoplatform.uexplorer.Address.unwrapped

import java.nio.file.Paths
import scala.jdk.CollectionConverters.*
import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent
import scala.util.Random

object SuperNodeUtils extends LazyLogging {

  def randomNumber(digits: Int): String = Random.alphanumeric.filter(_.isDigit).take(digits).mkString

  private val superNodeAddressFileName   = s"supernode-addresses-${randomNumber(5)}.csv"
  private val superNodeAddressFileNameGz = "supernode-addresses.csv.gz"

  // temporary for collecting supernode addresses in whole chain
  val superNodeAddresses: concurrent.Map[Address, Int] = new ConcurrentHashMap().asScala

  def superNodeAddressesWithPrefix: Map[Address, String] =
    Source
      .fromInputStream(
        new GZIPInputStream(
          new BufferedInputStream(
            Thread
              .currentThread()
              .getContextClassLoader
              .getResourceAsStream(superNodeAddressFileNameGz)
          )
        )
      )
      .getLines()
      .map(_.trim)
      .filterNot(_.isEmpty)
      .map(Address.fromStringUnsafe)
      .toSet
      .incl(FeeContract.address)
      .map(a => a -> a.unwrapped.take(32))
      .toMap

  def report(): Unit = {
    logger.info(s"Collected ${superNodeAddresses.size} supernode address : ")
    val addressesByCount =
      superNodeAddresses.toSeq
        .sortBy(_._2)
        .reverse
        .map { case (addr, count) =>
          s"$count $addr"
        }
    addressesByCount.foreach(println)
    addressesByCount.headOption.foreach { _ =>
      val destination = Paths.get(System.getProperty("java.io.tmpdir"), superNodeAddressFileName).toFile
      logger.info(s"Writing supernode addresses to ${destination.getAbsolutePath}")
      val fileWriter = new FileWriter(destination)
      try fileWriter.write(addressesByCount.mkString("", "\n", "\n"))
      finally fileWriter.close()
    }
  }
}
