package org.ergoplatform.uexplorer.indexer

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.{KillSwitches, SharedKillSwitch}
import org.ergoplatform.uexplorer.indexer.mempool.MempoolStateHolder.*
import org.ergoplatform.uexplorer.indexer.mempool.MempoolSyncer
import org.ergoplatform.uexplorer.indexer.plugin.PluginManager

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import org.ergoplatform.uexplorer.cassandra.AkkaStreamSupport
import org.ergoplatform.uexplorer.Resiliency
import org.ergoplatform.uexplorer.indexer.chain.{BlockIndexer, Initializer}
import org.ergoplatform.uexplorer.cassandra.api.Backend
import org.ergoplatform.uexplorer.indexer.chain.BlockIndexer.ChainSyncResult
import org.ergoplatform.uexplorer.indexer.chain.Initializer.*

class Scheduler(
  pluginManager: PluginManager,
  blockIndexer: BlockIndexer,
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
      ChainSyncResult(lastBlockOpt, utxoState, graphTraversalSource) <- blockIndexer.indexChain
      stateChanges                                                   <- mempoolSyncer.syncMempool(utxoState)
      _ <- pluginManager.executePlugins(utxoState, stateChanges, graphTraversalSource, lastBlockOpt)
    } yield stateChanges

  def validateAndSchedule(
    initialDelay: FiniteDuration,
    pollingInterval: FiniteDuration,
    verify: Boolean = true
  ): Future[Done] =
    initializer.init
      .flatMap {
        case GraphInconsistency(error) =>
          Future.failed(new IllegalStateException(error))
        case ChainEmpty | ChainValid(_) =>
          schedule(initialDelay, pollingInterval)(periodicSync).via(killSwitch.flow).run()
        case MissingBlocks(_, missingHeights) =>
          blockIndexer
            .fixChain(missingHeights)
            .flatMap(_ => validateAndSchedule(initialDelay, pollingInterval, verify = false))
      }
}
