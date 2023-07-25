package org.ergoplatform.uexplorer.indexer

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.CoreConf
import org.ergoplatform.uexplorer.backend.PersistentRepo
import org.ergoplatform.uexplorer.backend.blocks.{BlockRepo, PersistentBlockRepo}
import org.ergoplatform.uexplorer.backend.boxes.PersistentBoxRepo
import org.ergoplatform.uexplorer.config.ExplorerConfig
import org.ergoplatform.uexplorer.db.{FullBlock, GraphBackend}
import org.ergoplatform.uexplorer.http.*
import org.ergoplatform.uexplorer.indexer.chain.*
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import org.ergoplatform.uexplorer.indexer.db.{Backend, GraphBackend}
import org.ergoplatform.uexplorer.indexer.mempool.{MemPool, MempoolSyncer}
import org.ergoplatform.uexplorer.indexer.plugin.PluginManager
import org.ergoplatform.uexplorer.parser.ErgoTreeParser
import org.ergoplatform.uexplorer.plugin.Plugin
import org.ergoplatform.uexplorer.storage.{MvStorage, MvStoreConf}
import org.slf4j.LoggerFactory
import sttp.client3.httpclient.zio.HttpClientZioBackend
import zio.*
import zio.config.typesafe.TypesafeConfigProvider
import zio.logging.LogFormat
import zio.logging.backend.SLF4J

import java.io.{PrintWriter, StringWriter}
import java.util.ServiceLoader
import scala.collection.immutable.{ArraySeq, ListMap}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object ChainIndexer extends ZIOAppDefault {

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j(LogFormat.colored)

  def run =
    (for {
      serverFiber   <- Backend.runServer
      nodePoolFiber <- ZIO.serviceWithZIO[StreamScheduler](_.validateAndSchedule())
      done = serverFiber.zip(nodePoolFiber)
    } yield done).provide(
      ChainIndexerConf.layer,
      NodePoolConf.layer,
      MvStoreConf.layer,
      MemPool.layer,
      NodePool.layer,
      UnderlyingBackend.layer,
      SttpNodePoolBackend.layer,
      Backend.layerH2,
      MetadataHttpClient.layer,
      BlockHttpClient.layer,
      PersistentBlockRepo.layer,
      PersistentBoxRepo.layer,
      PersistentRepo.layer,
      StreamScheduler.layer,
      PluginManager.layer,
      StreamExecutor.layer,
      MempoolSyncer.layer,
      Initializer.layer,
      BlockReader.layer,
      BlockWriter.layer,
      GraphBackend.layer,
      MvStorage.zlayerWithDefaultDir
    )
}
