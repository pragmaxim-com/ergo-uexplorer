package org.ergoplatform.uexplorer.storage

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.{Address, ErgoTreeHex, ErgoTreeT8Hex}
import org.ergoplatform.uexplorer.parser.ErgoTreeParser

import java.io.{BufferedInputStream, FileWriter}
import java.util.zip.GZIPInputStream
import scala.io.Source
import scala.util.Success

object StorageUtils {
  implicit val enc: ErgoAddressEncoder = ErgoAddressEncoder(0.toByte)

  private def writeLinesToFile(lines: IterableOnce[String], filePath: String) = {
    val fileWriter = new FileWriter(filePath)
    try fileWriter.write(lines.iterator.mkString("", "\n", "\n"))
    finally fileWriter.close()

  }
  
  def ergoTreesFromAddresses: Set[ErgoTreeHex] =
    Source
      .fromInputStream(
        new GZIPInputStream(
          new BufferedInputStream(
            Thread
              .currentThread()
              .getContextClassLoader
              .getResourceAsStream("hot-addresses.csv.gz")
          )
        )
      )
      .getLines()
      .map(_.trim)
      .filterNot(_.isEmpty)
      .map(k => ErgoTreeParser.base58Address2ErgoTreeHex(Address.fromStringUnsafe(k)).get)
      .toSet

  def ergoTreeT8s: Set[ErgoTreeT8Hex] =
    Source
      .fromInputStream(
        new GZIPInputStream(
          new BufferedInputStream(
            Thread
              .currentThread()
              .getContextClassLoader
              .getResourceAsStream("hot-ergo-trees.csv.gz")
          )
        )
      )
      .getLines()
      .map(_.trim)
      .filterNot(_.isEmpty)
      .map(k => ErgoTreeParser.ergoTreeHex2T8Hex(ErgoTreeHex.fromStringUnsafe(k)))
      .collect { case Success(Some(t8)) => t8 }
      .toSet
    

}
