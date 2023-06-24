package org.ergoplatform.uexplorer.indexer

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.{KillSwitches, SharedKillSwitch}
import org.ergoplatform.uexplorer.Resiliency
import org.ergoplatform.uexplorer.cassandra.AkkaStreamSupport
import org.ergoplatform.uexplorer.cassandra.api.Backend
import org.ergoplatform.uexplorer.indexer.chain.StreamExecutor.ChainSyncResult
import org.ergoplatform.uexplorer.indexer.chain.Initializer.*
import org.ergoplatform.uexplorer.indexer.chain.{Initializer, StreamExecutor}
import org.ergoplatform.uexplorer.indexer.mempool.MempoolStateHolder.*
import org.ergoplatform.uexplorer.indexer.mempool.MempoolSyncer
import org.ergoplatform.uexplorer.indexer.plugin.PluginManager

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

class Scheduler(
  pluginManager: PluginManager,
  streamExecutor: StreamExecutor,
  mempoolSyncer: MempoolSyncer,
  initializer: Initializer
)(implicit
  s: ActorSystem[Nothing],
  mRef: ActorRef[MempoolStateHolderRequest],
  killSwitch: SharedKillSwitch
) extends AkkaStreamSupport
  with Resiliency {

  def periodicSync: Future[MempoolStateChanges] =
    for {
      ChainSyncResult(lastBlockOpt, storage, graphTraversalSource) <- streamExecutor.indexNewBlocks
      stateChanges                                                 <- mempoolSyncer.syncMempool(storage)
      _ <- pluginManager.executePlugins(storage, stateChanges, graphTraversalSource, lastBlockOpt)
    } yield stateChanges

  def validateAndSchedule(
    initialDelay: FiniteDuration,
    pollingInterval: FiniteDuration
  ): Future[Done] =
    initializer.init match {
      case HalfEmptyInconsistency(error) =>
        Future.failed(new IllegalStateException(error))
      case GraphInconsistency(error) =>
        Future.failed(new IllegalStateException(error))
      case ChainEmpty | ChainValid =>
        schedule(initialDelay, pollingInterval)(periodicSync).via(killSwitch.flow).run()
    }
}
