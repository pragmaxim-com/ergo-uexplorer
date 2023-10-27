package org.ergoplatform.uexplorer.indexer

import org.ergoplatform.uexplorer.CoreConf
import org.ergoplatform.uexplorer.backend.PersistentRepo
import org.ergoplatform.uexplorer.backend.blocks.{BlockService, PersistentBlockRepo}
import org.ergoplatform.uexplorer.backend.boxes.{BoxService, PersistentAssetRepo, PersistentBoxRepo}
import org.ergoplatform.uexplorer.backend.stats.StatsService
import org.ergoplatform.uexplorer.db.GraphBackend
import org.ergoplatform.uexplorer.http.*
import org.ergoplatform.uexplorer.indexer.chain.*
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import org.ergoplatform.uexplorer.indexer.db.Backend
import org.ergoplatform.uexplorer.indexer.mempool.{MemPool, MempoolSyncer}
import org.ergoplatform.uexplorer.indexer.plugin.PluginManager
import org.ergoplatform.uexplorer.storage.{MvStorage, MvStoreConf}
import zio.*
import zio.logging.LogFormat
import zio.logging.backend.SLF4J

import scala.language.postfixOps

object ChainIndexer extends ZIOAppDefault with ZioLayers {

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j(LogFormat.colored)

  def run =
    (for {
      serverFiber <- Backend.runServer.fork
      fiber       <- ZIO.serviceWithZIO[StreamScheduler](_.validateAndSchedule())
      done        <- fiber.zip(serverFiber).join
    } yield done).provide(
      httpLayer >+>
        storageLayer >+>
        databaseLayer >+>
        serviceLayers >+>
        PluginManager.layer >+>
        Initializer.layer >+>
        BlockReader.layer >+>
        BlockWriter.layer >+>
        StreamExecutor.layer >+>
        MemPool.layer >+>
        MempoolSyncer.layer >+>
        StreamScheduler.layer
    )
}
