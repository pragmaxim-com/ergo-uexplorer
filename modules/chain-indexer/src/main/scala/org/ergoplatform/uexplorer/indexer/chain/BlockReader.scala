package org.ergoplatform.uexplorer.indexer.chain

import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.http.{BlockHttpClient, Codecs}
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.{BlockId, Height}
import zio.ZLayer

import java.nio.file.{Path, Paths}
import scala.collection.immutable.TreeSet
import scala.concurrent.ExecutionContext.Implicits.global
import zio.stream.ZPipeline
import zio.stream.ZStream

class BlockReader(
  blockHttpClient: BlockHttpClient
) extends Codecs
  with LazyLogging {
  private val benchPath = Paths.get(System.getProperty("user.home"), ".ergo-uexplorer", "ergo-chain.lines.gz")

  def getBlockSource(fromHeight: Int, bench: Boolean): ZStream[Any, Throwable, ApiFullBlock] =
    if (bench)
      blockSourceFromFS.buffer(1024)
    else
      blockIdSource(fromHeight)
        .buffer(1024)
        .mapZIOPar(1)(blockHttpClient.getBlockForId) // parallelism could be parameterized - low or big pressure on Node
        .buffer(1024)

  def blockSourceFromFS: ZStream[Any, Throwable, ApiFullBlock] = {
    import io.circe.parser.decode
    logger.info(s"Running benchmark, reading all blocks from filesystem")
    ZStream
      .fromFile(benchPath.toFile())
      .via(ZPipeline.gunzip() >>> ZPipeline.utf8Decode >>> ZPipeline.splitLines)
      .map(s => decode[ApiFullBlock](s).toTry.get)
  }

  def blockIdSource(fromHeight: Int): ZStream[Any, Throwable, BlockId] =
    ZStream
      .unfoldZIO(fromHeight) { offset =>
        blockHttpClient.getBlockIdsByOffset(offset, 100).map {
          case blockIds if blockIds.nonEmpty =>
            logger.info(s"Height $offset")
            Some((blockIds, offset + blockIds.size))
          case _ =>
            None
        }
      }
      .buffer(10)
      .mapConcat(identity)

}

object BlockReader {
  def layer: ZLayer[BlockHttpClient, Nothing, BlockReader] =
    ZLayer.fromFunction(BlockReader(_))

}
