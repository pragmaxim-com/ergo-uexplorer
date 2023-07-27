package org.ergoplatform.uexplorer.storage.tool

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.parser.ErgoTreeParser
import org.ergoplatform.uexplorer.{Address, ErgoTreeHex, ErgoTreeT8Hex}
import zio.{Task, URIO, ZIO}

import java.io.{BufferedInputStream, FileWriter}
import java.util.zip.GZIPInputStream
import scala.io.Source
import scala.util.Success

object StorageUtils {
  implicit val enc: ErgoAddressEncoder = ErgoAddressEncoder(0.toByte)

  def writeLinesToFile(lines: IterableOnce[String], filePath: String) = {
    val fileWriter = new FileWriter(filePath)
    try fileWriter.write(lines.iterator.mkString("", "\n", "\n"))
    finally fileWriter.close()

  }

  def ergoTreesFromAddresses: Task[Set[ErgoTreeHex]] =
    ZIO.collectAll(
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
        .map(k => ErgoTreeParser.base58Address2ErgoTreeHex(Address.fromStringUnsafe(k)))
        .toSet
    )

  def ergoTreeT8s: Task[Set[Option[ErgoTreeT8Hex]]] =
    ZIO.collectAll(
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
        .toSet
    )

}
