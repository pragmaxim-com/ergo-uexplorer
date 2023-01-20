package org.ergoplatform.uexplorer.indexer

import akka.Done
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.KillSwitches
import org.ergoplatform.uexplorer.indexer.chain.ChainIndexer.ChainSyncResult
import org.ergoplatform.uexplorer.indexer.chain.ChainLoader.{ChainValid, MissingEpochs}
import org.ergoplatform.uexplorer.indexer.chain.ChainStateHolder.ChainStateHolderRequest
import org.ergoplatform.uexplorer.indexer.chain.{ChainIndexer, ChainLoader, ChainState, ChainStateHolder}
import org.ergoplatform.uexplorer.indexer.mempool.MempoolStateHolder.*
import org.ergoplatform.uexplorer.indexer.mempool.MempoolSyncer
import org.ergoplatform.uexplorer.indexer.plugin.PluginManager

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure

class Scheduler(
  pluginManager: PluginManager,
  chainIndexer: ChainIndexer,
  mempoolSyncer: MempoolSyncer,
  chainLoader: ChainLoader
)(implicit s: ActorSystem[Nothing], cRef: ActorRef[ChainStateHolderRequest], mRef: ActorRef[MempoolStateHolderRequest])
  extends AkkaStreamSupport {

  private lazy val killSwitch = KillSwitches.shared("scheduler")

  def periodicSync: Future[(ChainState, MempoolStateChanges)] =
    for {
      ChainSyncResult(chainState, lastBlock, gts, topAddressMap) <- chainIndexer.indexChain
      stateChanges                                               <- mempoolSyncer.syncMempool(chainState)
      _                                                          <- pluginManager.executePlugins(chainState, stateChanges, gts, lastBlock, topAddressMap)
    } yield (chainState, stateChanges)

  def validateAndSchedule(
    initialDelay: FiniteDuration,
    pollingInterval: FiniteDuration,
    verify: Boolean = true
  ): Future[Done] =
    chainLoader
      .initFromDbAndDisk(verify)
      .flatMap {
        case ChainValid(_) =>
          schedule(initialDelay, pollingInterval, killSwitch)(periodicSync).run()
        case missingEpochs: MissingEpochs =>
          chainIndexer
            .fixChain(missingEpochs)
            .flatMap(_ => validateAndSchedule(initialDelay, pollingInterval, verify = false))
      }

}
