package org.ergoplatform.uexplorer.indexer.chain

import akka.actor.CoordinatedShutdown
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.{ActorAttributes, KillSwitches, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.Resiliency
import org.ergoplatform.uexplorer.indexer.api.{Backend, InMemoryBackend, UtxoSnapshotManager}
import org.ergoplatform.uexplorer.indexer.cassandra.CassandraBackend
import org.ergoplatform.uexplorer.indexer.chain.ChainIndexer.ChainSyncResult
import org.ergoplatform.uexplorer.indexer.chain.ChainLoader.{ChainValid, MissingEpochs}
import org.ergoplatform.uexplorer.indexer.chain.ChainStateHolder.*
import org.ergoplatform.uexplorer.indexer.chain.{ChainLoader, ChainState, ChainStateHolder, Epoch}
import org.ergoplatform.uexplorer.indexer.config.{CassandraDb, ChainIndexerConf, InMemoryDb, ProtocolSettings}
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.mempool.MempoolStateHolder
import org.ergoplatform.uexplorer.indexer.mempool.MempoolStateHolder.*
import org.ergoplatform.uexplorer.indexer.plugin.PluginManager
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState
import org.ergoplatform.uexplorer.plugin.Plugin
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateWithPool, UtxoStateWithoutPool}
import org.ergoplatform.uexplorer.{Address, BoxId, Const, Height}

import java.util.ServiceLoader
import scala.collection.immutable.{ArraySeq, ListMap}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Try}

class ChainIndexer(
  backend: Backend,
  blockHttpClient: BlockHttpClient,
  snapshotManager: UtxoSnapshotManager
)(implicit s: ActorSystem[Nothing], ref: ActorRef[ChainStateHolderRequest])
  extends LazyLogging {
  private lazy val killSwitch = KillSwitches.shared("indexer")

  CoordinatedShutdown(s).addTask(
    CoordinatedShutdown.PhaseBeforeServiceUnbind,
    "stop-akka-stream"
  ) { () =>
    Future {
      killSwitch.shutdown()
      Done
    }
  }

  private val indexingSink: Sink[Height, Future[ChainSyncResult]] =
    Flow[Height]
      .via(blockHttpClient.blockCachingFlow)
      .via(backend.blockWriteFlow)
      .mapAsync(1) {
        case block if Epoch.heightAtFlushPoint(block.header.height) =>
          ChainStateHolder
            .finishEpoch(Epoch.epochIndexForHeight(block.header.height) - 1)
            .map(e => block -> Option(e))
        case block =>
          Future.successful(block -> Option.empty)
      }
      .async
      .buffer(2, OverflowStrategy.backpressure)
      .via(backend.addressWriteFlow)
      .async
      .buffer(2, OverflowStrategy.backpressure)
      .via(backend.graphWriteFlow)
      .via(backend.epochsWriteFlow)
      .via(killSwitch.flow)
      .withAttributes(supervisionStrategy(Resiliency.decider(killSwitch)))
      .toMat(
        Sink
          .fold((Option.empty[Block], Option.empty[NewEpochDetected])) {
            case (_, (block, Some(e @ NewEpochDetected(_, _, _)))) =>
              Option(block) -> Option(e)
            case ((_, lastEpoch), (block, _)) =>
              Option(block) -> lastEpoch
          }
      ) { case (_, mat) =>
        mat.flatMap { case (lastBlock, lastEpoch) =>
          ChainStateHolder.getChainState.flatMap { chainState =>
            snapshotManager
              .makeSnapshotOnEpoch(lastEpoch.map(_.epoch), chainState.utxoState)
              .map { _ =>
                ChainSyncResult(chainState, lastBlock, backend.graphTraversalSource)
              }
          }
        }
      }

  def indexChain: Future[ChainSyncResult] = for {
    bestBlockHeight <- blockHttpClient.getBestBlockHeight
    chainState      <- getChainState
    fromHeight = chainState.getLastCachedBlock.map(_.height).getOrElse(0) + 1
    _          = if (bestBlockHeight > fromHeight) logger.info(s"Going to index blocks from $fromHeight to $bestBlockHeight")
    _          = if (bestBlockHeight == fromHeight) logger.info(s"Going to index block $bestBlockHeight")
    syncResult <- Source(fromHeight to bestBlockHeight).runWith(indexingSink)
  } yield syncResult

  def fixChain(missingEpochs: MissingEpochs): Future[ChainSyncResult] =
    Source(missingEpochs.missingHeights)
      .runWith(indexingSink)
}

object ChainIndexer {
  case class ChainSyncResult(chainState: ChainState, lastBlock: Option[Block], graphTraversalSource: GraphTraversalSource)
}
