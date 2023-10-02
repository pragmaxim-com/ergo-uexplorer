package org.ergoplatform.uexplorer.indexer

import org.ergoplatform.uexplorer.ReadableStorage
import org.ergoplatform.uexplorer.chain.ChainLinker
import org.ergoplatform.uexplorer.http.{BlockHttpClient, SttpNodePoolBackend}
import org.ergoplatform.uexplorer.indexer.chain.Initializer.*
import org.ergoplatform.uexplorer.indexer.chain.{Initializer, StreamExecutor}
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import org.ergoplatform.uexplorer.indexer.mempool.{MemPoolStateChanges, MempoolSyncer}
import org.ergoplatform.uexplorer.indexer.plugin.PluginManager
import org.ergoplatform.uexplorer.storage.MvStoreConf
import zio.*

case class StreamScheduler(
  pluginManager: PluginManager,
  streamExecutor: StreamExecutor,
  mempoolSyncer: MempoolSyncer,
  initializer: Initializer,
  storage: ReadableStorage,
  blockHttpClient: BlockHttpClient,
  nodePoolBackend: SttpNodePoolBackend,
  conf: ChainIndexerConf
) {

  def periodicSync(chainLinker: ChainLinker): Task[MemPoolStateChanges] =
    for {
      lastHeight      <- ZIO.attempt(storage.getLastHeight.getOrElse(0) + 1)
      chainSyncResult <- streamExecutor.indexNewBlocks(chainLinker, lastHeight, conf.benchmarkMode).logError("Syncing chain failed")
      stateChanges    <- mempoolSyncer.syncMempool.logError("Syncing mempool failed")
      _ <- pluginManager
             .executePlugins(
               chainSyncResult.storage,
               stateChanges,
               chainSyncResult.graphTraversalSource,
               chainSyncResult.lastBlock
             )
             .logError("Plugin execution failed")
    } yield stateChanges

  def validateAndSchedule(
    schedule: Schedule[Any, Any, Any] = Schedule.fixed(conf.mvStore.maxIdleCompactTime + 5.seconds)
  ): ZIO[Any, Throwable, Fiber.Runtime[Throwable, Long]] =
    initializer.init.flatMap {
      case HalfEmptyInconsistency(error) =>
        ZIO.fail(new IllegalStateException(error))
      case GraphInconsistency(error) =>
        ZIO.fail(new IllegalStateException(error))
      case ChainEmpty =>
        for {
          chainTip <- storage.getChainTip
          chainLinker = ChainLinker(blockHttpClient.getBlockForId, chainTip)(conf.core)
          _     <- ZIO.log(s"Chain is empty, loading from scratch ...")
          fiber <- nodePoolBackend.keepNodePoolUpdated
          _     <- periodicSync(chainLinker).repeatOrElse(schedule, (_, _) => nodePoolBackend.cleanNodePool *> validateAndSchedule(schedule))
        } yield fiber

      case ChainValid =>
        for {
          chainTip <- storage.getChainTip
          chainLinker = ChainLinker(blockHttpClient.getBlockForId, chainTip)(conf.core)
          fiber <- nodePoolBackend.keepNodePoolUpdated
          _     <- periodicSync(chainLinker).repeatOrElse(schedule, (_, _) => nodePoolBackend.cleanNodePool *> validateAndSchedule(schedule))
        } yield fiber
    }
}

object StreamScheduler {
  def layer: ZLayer[
    BlockHttpClient
      with ChainIndexerConf
      with ReadableStorage
      with SttpNodePoolBackend
      with PluginManager
      with StreamExecutor
      with MempoolSyncer
      with Initializer,
    Nothing,
    StreamScheduler
  ] =
    ZLayer.fromFunction(StreamScheduler.apply _)

}
