package org.ergoplatform.uexplorer.indexer.tool

import org.ergoplatform.uexplorer.http.*
import org.ergoplatform.uexplorer.indexer.chain.{BlockReader, StreamExecutor}
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import sttp.client3.httpclient.zio.HttpClientZioBackend

import java.io.{PrintWriter, StringWriter}
import java.nio.file.Paths
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
import zio.*
import zio.stream.*

object DownloadBlocksAsLinesFromNodeToFile extends ZIOAppDefault {

  private val targetFile = Paths.get(java.lang.System.getProperty("user.home"), ".ergo-uexplorer", "ergo-chain.100k.gz")
  targetFile.toFile.getParentFile.mkdirs()

  def run =
    (for
      blockHttpClient <- ZIO.service[BlockHttpClient]
      blockReader     <- ZIO.service[BlockReader]
      _               <- ZIO.log(s"Initiating download")
      result <- blockReader
                  .blockIdSource(1)
                  .take(100000)
                  .mapZIO(blockHttpClient.getBlockForIdAsString)
                  .mapConcat(line => (line + java.lang.System.lineSeparator()).getBytes)
                  .via(ZPipeline.gzip())
                  .run(ZSink.fromFile(targetFile.toFile))
    yield result).provide(
      HttpClientZioBackend.layer(),
      MetadataHttpClient.layer,
      UnderlyingBackend.layer,
      BlockHttpClient.layer,
      BlockReader.layer,
      ChainIndexerConf.layer.project(_.nodePool)
    )
}
