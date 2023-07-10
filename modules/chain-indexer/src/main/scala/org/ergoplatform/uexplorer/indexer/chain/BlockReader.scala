package org.ergoplatform.uexplorer.indexer.chain

import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Compression, FileIO, Flow, Framing, Sink, Source}
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.http.{BlockHttpClient, Codecs}
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.{BlockId, Height}

import java.nio.file.{Path, Paths}
import scala.collection.immutable.TreeSet
import scala.concurrent.ExecutionContext.Implicits.global

class BlockReader(
  blockHttpClient: BlockHttpClient
) extends Codecs
  with LazyLogging {
  private val benchPath = Paths.get(System.getProperty("user.home"), ".ergo-uexplorer", "ergo-chain.lines.gz")

  private val blockForIdFlow: Flow[BlockId, ApiFullBlock, NotUsed] =
    Flow[BlockId]
      .mapAsync(1)(blockHttpClient.getBlockForId) // parallelism could be parameterized - low or big pressure on Node
      .buffer(4096, OverflowStrategy.backpressure)

  def getBlockSource(fromHeight: Int, bench: Boolean): Source[ApiFullBlock, NotUsed] =
    if (bench) blockSourceFromFS
    else blockIdSource(fromHeight).via(blockForIdFlow)

  def blockSourceFromFS: Source[ApiFullBlock, NotUsed] = {
    import io.circe.parser.decode
    logger.info(s"Running benchmark, reading all blocks from filesystem")
    FileIO
      .fromPath(benchPath)
      .via(Compression.gunzip())
      .via(Framing.delimiter(ByteString("\n"), 512 * 1000))
      .map(_.utf8String)
      .map(s => decode[ApiFullBlock](s).toTry.get)
      .mapMaterializedValue(_ => NotUsed)
  }

  def blockIdSource(fromHeight: Int): Source[BlockId, NotUsed] =
    Source
      .unfoldAsync(fromHeight) { offset =>
        blockHttpClient.getBlockIdsByOffset(offset, 100).map {
          case blockIds if blockIds.nonEmpty =>
            logger.info(s"Height $offset")
            Some((offset + blockIds.size, blockIds))
          case _ =>
            None
        }
      }
      .buffer(10, OverflowStrategy.backpressure)
      .mapConcat(identity)

}
