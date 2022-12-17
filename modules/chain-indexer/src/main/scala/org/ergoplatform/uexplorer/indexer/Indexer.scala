package org.ergoplatform.uexplorer.indexer

import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.{Address, BoxId}
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.api.{Backend, InMemoryBackend}
import org.ergoplatform.uexplorer.indexer.cassandra.CassandraBackend
import org.ergoplatform.uexplorer.indexer.config.{CassandraDb, ChainIndexerConf, InMemoryDb, ProtocolSettings}
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.chain.ChainSyncer.*
import org.ergoplatform.uexplorer.indexer.chain.{ChainState, ChainSyncer, Epoch}
import org.ergoplatform.uexplorer.indexer.mempool.MempoolSyncer
import org.ergoplatform.uexplorer.indexer.mempool.MempoolSyncer.{MempoolState, MempoolStateChanges, MempoolSyncerRequest}
import org.ergoplatform.uexplorer.indexer.plugin.PluginManager
import org.ergoplatform.uexplorer.indexer.utxo.{UtxoSnapshot, UtxoSnapshotManager, UtxoState}
import org.ergoplatform.uexplorer.plugin.Plugin
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateWithPool, UtxoStateWithoutPool}

import scala.jdk.CollectionConverters.*
import java.util.ServiceLoader
import scala.collection.immutable.{ArraySeq, ListMap}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Try}

class Indexer(
  backend: Backend,
  blockHttpClient: BlockHttpClient,
  snapshotManager: UtxoSnapshotManager,
  plugins: List[Plugin]
)(implicit
  val s: ActorSystem[Nothing],
  chainSyncerRef: ActorRef[ChainSyncerRequest],
  mempoolSyncerRef: ActorRef[MempoolSyncerRequest]
) extends AkkaStreamSupport
  with LazyLogging {

  private val blockToEpochFlow: Flow[Block, (Block, Option[MaybeNewEpoch]), NotUsed] =
    Flow[Block]
      .mapAsync(1) {
        case block if Epoch.heightAtFlushPoint(block.header.height) =>
          ChainSyncer
            .finishEpoch(Epoch.epochIndexForHeight(block.header.height) - 1)
            .map(e => block -> Option(e))
        case block =>
          Future.successful(block -> Option.empty)
      }

  private val indexingFlow: Flow[Int, (Block, Option[MaybeNewEpoch]), NotUsed] =
    Flow[Int]
      .via(blockHttpClient.blockCachingFlow)
      .async
      .via(backend.blockWriteFlow)
      .via(blockToEpochFlow)
      .via(backend.epochsWriteFlow)
      .withAttributes(supervisionStrategy(Resiliency.decider))

  private val indexingSink: Sink[(Block, Option[MaybeNewEpoch]), Future[(Option[Block], Option[NewEpochCreated])]] =
    Sink.fold((Option.empty[Block], Option.empty[NewEpochCreated])) {
      case ((_, _), (block, Some(e @ NewEpochCreated(_)))) =>
        Option(block) -> Option(e)
      case ((_, lastEpoch), (block, _)) =>
        Option(block) -> lastEpoch
    }

  def verifyStateIntegrity(chainState: ChainState): Future[Done] = {
    val missingHeights = chainState.findMissingIndexes.flatMap(Epoch.heightRangeForEpochIndex)
    if (missingHeights.nonEmpty) {
      logger.error(s"Going to index missing blocks at heights : ${missingHeights.mkString(", ")}")
      Source(missingHeights)
        .via(indexingFlow)
        .run()
        .transform { _ =>
          snapshotManager.clearAllSnapshots
          Failure(new UnexpectedStateError("App restart is needed to reload UtxoState"))
        }
    } else {
      logger.info(s"Chain state is valid")
      Future.successful(Done)
    }
  }

  def syncChain(bestBlockHeight: Int): Future[(Option[Block], Option[NewEpochCreated])] = for {
    chainState <- ChainSyncer.getChainState
    fromHeight = chainState.getLastCachedBlock.map(_.height).getOrElse(0) + 1
    _          = if (bestBlockHeight > fromHeight) logger.info(s"Going to index blocks from $fromHeight to $bestBlockHeight")
    _          = if (bestBlockHeight == fromHeight) logger.info(s"Going to index block $bestBlockHeight")
    lastElements <- Source(fromHeight to bestBlockHeight).via(indexingFlow).runWith(indexingSink)
  } yield lastElements

  def periodicSync: Future[(ChainState, MempoolStateChanges)] =
    for {
      bestBlockHeight              <- blockHttpClient.getBestBlockHeight
      (lastBlockOpt, lastEpochOpt) <- syncChain(bestBlockHeight)
      chainState                   <- ChainSyncer.getChainState
      _                            <- snapshotManager.makeSnapshotOnEpoch(lastEpochOpt, chainState.utxoState)
      stateChanges                 <- MempoolSyncer.syncMempool(blockHttpClient, chainState, bestBlockHeight)
      _                            <- PluginManager.executePlugins(plugins, chainState, stateChanges, lastBlockOpt)
    } yield (chainState, stateChanges)

  def run(initialDelay: FiniteDuration, pollingInterval: FiniteDuration): Future[Done] =
    for {
      chainState <- ChainState.load(backend, snapshotManager)
      _          <- ChainSyncer.initialize(chainState)
      _          <- verifyStateIntegrity(chainState)
      done       <- schedule(initialDelay, pollingInterval)(periodicSync).run()
    } yield done
}

object Indexer extends LazyLogging {

  def runWith(
    conf: ChainIndexerConf
  )(implicit ctx: ActorContext[Nothing]): Future[Done] = {
    implicit val system: ActorSystem[Nothing]                 = ctx.system
    implicit val protocol: ProtocolSettings                   = conf.protocol
    implicit val chainSyncerRef: ActorRef[ChainSyncerRequest] = ctx.spawn(new ChainSyncer().initialBehavior, "ChainSyncer")
    implicit val mempoolSyncerRef: ActorRef[MempoolSyncerRequest] =
      ctx.spawn(MempoolSyncer.behavior(MempoolState(ListMap.empty)), "MempoolSyncer")
    for {
      blockHttpClient <- BlockHttpClient.withNodePoolBackend(conf)
      plugins         <- Future.fromTry(PluginManager.loadPlugins)
      _ = if (plugins.nonEmpty) logger.info(s"Plugins loaded: ${plugins.map(_.name).mkString(", ")}")
      _ <- Future.sequence(plugins.map(_.init))
      indexer =
        conf.backendType match {
          case CassandraDb(parallelism) =>
            new Indexer(CassandraBackend(parallelism), blockHttpClient, new UtxoSnapshotManager(), plugins)
          case InMemoryDb =>
            new Indexer(new InMemoryBackend(), blockHttpClient, new UtxoSnapshotManager(), plugins)
        }
      done <- indexer
                .run(0.seconds, 5.seconds)
                .andThen { case Failure(ex) =>
                  logger.error(s"Shutting down due to unexpected error", ex)
                  Future
                    .sequence(blockHttpClient.close() :: plugins.map(_.close))
                    .andThen { case _ => system.terminate() }
                }

    } yield done
  }
}
