package org.ergoplatform.uexplorer.indexer.chain

import org.ergoplatform.uexplorer.http.{BlockHttpClient, Codecs}
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.BlockId
import zio.ZLayer

import java.nio.file.Paths
import zio.stream.ZPipeline
import zio.stream.ZStream

class BlockReader(
  blockHttpClient: BlockHttpClient
) extends Codecs {
  private val benchPath = Paths.get(System.getProperty("user.home"), ".ergo-uexplorer", "ergo-chain.lines.gz")

  def getBlockSource(fromHeight: Int, bench: Boolean): ZStream[Any, Throwable, ApiFullBlock] =
    if (bench && benchPath.toFile.exists())
      blockSourceFromFS(fromHeight).buffer(128)
    else
      blockIdSource(fromHeight)
        .buffer(128)
        .mapZIOPar(1)(blockHttpClient.getBlockForId) // parallelism could be parameterized - low or big pressure on Node
        .buffer(32)

  def blockSourceFromFS(fromHeight: Int): ZStream[Any, Throwable, ApiFullBlock] = {
    import io.circe.parser.decode
    val stream =
      ZStream
        .fromFile(benchPath.toFile())
        .via(ZPipeline.gunzip() >>> ZPipeline.utf8Decode >>> ZPipeline.splitLines)
        .map(s => decode[ApiFullBlock](s).toTry.get)
    if (fromHeight == 1)
      stream
    else
      stream.dropUntil(_.header.height == fromHeight - 1)
  }

  def blockIdSource(fromHeight: Int): ZStream[Any, Throwable, BlockId] =
    ZStream
      .unfoldZIO(fromHeight) { offset =>
        blockHttpClient.getBlockIdsByOffset(offset, 100).map {
          case blockIds if blockIds.nonEmpty =>
            Some((blockIds, offset + blockIds.size))
          case _ =>
            None
        }
      }
      .mapConcat(identity)

}

object BlockReader {
  def layer: ZLayer[BlockHttpClient, Nothing, BlockReader] =
    ZLayer.fromFunction(BlockReader(_))

}
