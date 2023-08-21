package org.ergoplatform.uexplorer.indexer

import org.ergoplatform.uexplorer.http.SttpNodePoolBackend
import org.ergoplatform.uexplorer.indexer.chain.Initializer.*
import org.ergoplatform.uexplorer.indexer.chain.{Initializer, StreamExecutor}
import org.ergoplatform.uexplorer.indexer.mempool.{MemPoolStateChanges, MempoolSyncer}
import org.ergoplatform.uexplorer.indexer.plugin.PluginManager
import org.ergoplatform.uexplorer.storage.MvStoreConf
import zio.*

case class StreamScheduler(
  pluginManager: PluginManager,
  streamExecutor: StreamExecutor,
  mempoolSyncer: MempoolSyncer,
  initializer: Initializer,
  nodePoolBackend: SttpNodePoolBackend,
  conf: MvStoreConf
) {

  def periodicSync: Task[MemPoolStateChanges] =
    for {
      chainSyncResult <- streamExecutor.indexNewBlocks.logError("Syncing chain failed")
      stateChanges    <- mempoolSyncer.syncMempool(chainSyncResult.storage).logError("Syncing mempool failed")
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
    schedule: Schedule[Any, Any, Any] = Schedule.fixed(conf.maxIdleCompactTime + 5.seconds)
  ): ZIO[Any, Throwable, Fiber.Runtime[Throwable, Long]] =
    initializer.init.flatMap {
      case HalfEmptyInconsistency(error) =>
        ZIO.fail(new IllegalStateException(error))
      case GraphInconsistency(error) =>
        ZIO.fail(new IllegalStateException(error))
      case ChainEmpty =>
        for {
          _     <- ZIO.log(s"Chain is empty, loading from scratch ...")
          fiber <- nodePoolBackend.keepNodePoolUpdated
          _     <- periodicSync.repeat(schedule).ignore
        } yield fiber

      case ChainValid =>
        for {
          fiber <- nodePoolBackend.keepNodePoolUpdated
          _     <- periodicSync.repeat(schedule).ignore
        } yield fiber
    }
}

object StreamScheduler {
  def layer: ZLayer[
    SttpNodePoolBackend with PluginManager with StreamExecutor with MempoolSyncer with Initializer with MvStoreConf,
    Nothing,
    StreamScheduler
  ] =
    ZLayer.fromFunction(StreamScheduler.apply _)

}
