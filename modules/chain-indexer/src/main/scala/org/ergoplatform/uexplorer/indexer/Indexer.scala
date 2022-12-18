package org.ergoplatform.uexplorer.indexer

import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.Indexer.ChainSyncResult
import org.ergoplatform.uexplorer.indexer.api.{Backend, InMemoryBackend}
import org.ergoplatform.uexplorer.indexer.cassandra.CassandraBackend
import org.ergoplatform.uexplorer.indexer.chain.ChainSyncer.*
import org.ergoplatform.uexplorer.indexer.chain.{ChainState, ChainSyncer, Epoch}
import org.ergoplatform.uexplorer.indexer.config.{CassandraDb, ChainIndexerConf, InMemoryDb, ProtocolSettings}
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.mempool.MempoolSyncer
import org.ergoplatform.uexplorer.indexer.mempool.MempoolSyncer.{MempoolState, MempoolStateChanges, MempoolSyncerRequest}
import org.ergoplatform.uexplorer.indexer.plugin.PluginManager
import org.ergoplatform.uexplorer.indexer.utxo.{UtxoSnapshot, UtxoSnapshotManager, UtxoState}
import org.ergoplatform.uexplorer.plugin.Plugin
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateWithPool, UtxoStateWithoutPool}
import org.ergoplatform.uexplorer.{Address, BoxId, Const}

import java.util.ServiceLoader
import scala.collection.immutable.{ArraySeq, ListMap}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters.*
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

  private val indexingSink: Sink[Int, Future[ChainSyncResult]] =
    Flow[Int]
      .via(blockHttpClient.blockCachingFlow)
      .async
      .via(backend.blockWriteFlow)
      .mapAsync(1) {
        case block if Epoch.heightAtFlushPoint(block.header.height) =>
          ChainSyncer
            .finishEpoch(Epoch.epochIndexForHeight(block.header.height) - 1)
            .map(e => block -> Option(e))
        case block =>
          Future.successful(block -> Option.empty)
      }
      .via(backend.epochsWriteFlow)
      .withAttributes(supervisionStrategy(Resiliency.decider))
      .toMat(
        Sink
          .fold((Option.empty[Block], Option.empty[NewEpochCreated])) {
            case (_, (block, Some(e @ NewEpochCreated(_)))) =>
              Option(block) -> Option(e)
            case ((_, lastEpoch), (block, _)) =>
              Option(block) -> lastEpoch
          }
      ) { case (_, mat) =>
        mat.flatMap { case (lastBlock, lastEpoch) =>
          ChainSyncer.getChainState.flatMap { chainState =>
            snapshotManager.makeSnapshotOnEpoch(lastEpoch, chainState.utxoState).map { _ =>
              ChainSyncResult(chainState, lastBlock)
            }
          }
        }
      }

  def periodicSync: Future[(ChainState, MempoolStateChanges)] =
    for {
      ChainSyncResult(chainState, lastBlock) <- ChainSyncer.syncChain(blockHttpClient, indexingSink)
      stateChanges                           <- MempoolSyncer.syncMempool(blockHttpClient, chainState)
      _                                      <- PluginManager.executePlugins(plugins, chainState, stateChanges, lastBlock)
    } yield (chainState, stateChanges)

  def validateAndSchedule(initialDelay: FiniteDuration, pollingInterval: FiniteDuration): Future[Done] =
    ChainSyncer.initFromDbAndDisk(backend, snapshotManager).flatMap {
      case ChainValid =>
        schedule(initialDelay, pollingInterval)(periodicSync).run()
      case missingEpochs: MissingEpochs =>
        Source(missingEpochs.missingHeights)
          .runWith(indexingSink)
          .flatMap(_ => validateAndSchedule(initialDelay, pollingInterval))
    }
}

object Indexer extends LazyLogging {

  case class ChainSyncResult(chainState: ChainState, lastBlock: Option[Block])

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
                .validateAndSchedule(0.seconds, 5.seconds)
                .andThen { case Failure(ex) =>
                  logger.error(s"Shutting down due to unexpected error", ex)
                  Future
                    .sequence(blockHttpClient.close() :: plugins.map(_.close))
                    .andThen { case _ => system.terminate() }
                }

    } yield done
  }
}
