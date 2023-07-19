package org.ergoplatform.uexplorer.indexer

import org.ergoplatform.uexplorer.ExeContext.Implicits
import org.ergoplatform.uexplorer.http.SttpNodePoolBackend
import org.ergoplatform.uexplorer.indexer.chain.Initializer.*
import org.ergoplatform.uexplorer.indexer.chain.StreamExecutor.ChainSyncResult
import org.ergoplatform.uexplorer.indexer.chain.{Initializer, StreamExecutor}
import org.ergoplatform.uexplorer.indexer.db.Backend
import org.ergoplatform.uexplorer.indexer.plugin.PluginManager
import zio.*
import org.ergoplatform.uexplorer.indexer.mempool.MemPoolStateChanges

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import org.ergoplatform.uexplorer.indexer.mempool.MempoolSyncer
import org.ergoplatform.uexplorer.storage.MvStoreConf

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
      chainSyncResult <- streamExecutor.indexNewBlocks
      stateChanges    <- mempoolSyncer.syncMempool(chainSyncResult.storage)
      _ <- pluginManager.executePlugins(
             chainSyncResult.storage,
             stateChanges,
             chainSyncResult.graphTraversalSource,
             chainSyncResult.lastBlock
           )
    } yield stateChanges

  def validateAndSchedule(
    schedule: Schedule[Any, Any, Any] = Schedule.fixed(conf.maxIdleCompactTime + 5.seconds)
  ): Task[Fiber.Runtime[Throwable, Long]] =
    initializer.init.flatMap {
      case HalfEmptyInconsistency(error) =>
        ZIO.fail(new IllegalStateException(error))
      case GraphInconsistency(error) =>
        ZIO.fail(new IllegalStateException(error))
      case ChainEmpty =>
        for {
          _     <- ZIO.log(s"Chain is empty, loading from scratch ...")
          fiber <- nodePoolBackend.keepNodePoolUpdated
          _     <- periodicSync.repeat(schedule)
        } yield fiber

      case ChainValid =>
        for {
          _     <- ZIO.log(s"Chain is valid, continue loading ...")
          fiber <- nodePoolBackend.keepNodePoolUpdated
          _     <- periodicSync.repeat(schedule)
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
