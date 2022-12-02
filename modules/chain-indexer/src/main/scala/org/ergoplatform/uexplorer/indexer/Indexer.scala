package org.ergoplatform.uexplorer.indexer

import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.scaladsl.{Flow, Source}
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.api.{Backend, InMemoryBackend}
import org.ergoplatform.uexplorer.indexer.cassandra.CassandraBackend
import org.ergoplatform.uexplorer.indexer.config.{CassandraDb, ChainIndexerConf, InMemoryDb, ProtocolSettings}
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.chain.ChainSyncer.*
import org.ergoplatform.uexplorer.indexer.chain.{ChainState, ChainSyncer, Epoch, UtxoState}
import org.ergoplatform.uexplorer.indexer.mempool.MempoolSyncer
import org.ergoplatform.uexplorer.indexer.mempool.MempoolSyncer.{MempoolState, MempoolSyncerRequest, NewTransactions}
import org.ergoplatform.uexplorer.plugin.Plugin

import scala.jdk.CollectionConverters.*
import java.util.ServiceLoader
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Try}

class Indexer(backend: Backend, blockHttpClient: BlockHttpClient)(implicit
  val s: ActorSystem[Nothing],
  chainSyncerRef: ActorRef[ChainSyncerRequest],
  mempoolSyncerRef: ActorRef[MempoolSyncerRequest]
) extends AkkaStreamSupport
  with LazyLogging {

  val blockToEpochFlow: Flow[Block, (Block, Option[MaybeNewEpoch]), NotUsed] =
    Flow[Block]
      .mapAsync(1) {
        case block if Epoch.heightAtFlushPoint(block.header.height) =>
          ChainSyncer
            .finishEpoch(Epoch.epochIndexForHeight(block.header.height) - 1)
            .map(e => block -> Option(e))
        case block =>
          Future.successful(block -> Option.empty)
      }

  val indexingFlow: Flow[Int, (Block, Option[MaybeNewEpoch]), NotUsed] =
    Flow[Int]
      .via(blockHttpClient.blockCachingFlow)
      .async
      .via(backend.blockWriteFlow)
      .via(blockToEpochFlow)
      .via(backend.epochsWriteFlow)
      .withAttributes(supervisionStrategy(Resiliency.decider))

  def syncMempool(chainState: ChainState, bestBlockHeight: Int): Future[NewTransactions] =
    if (chainState.blockBuffer.byHeight.lastOption.map(_._1).exists(_ >= bestBlockHeight)) {
      for {
        txs    <- blockHttpClient.getUnconfirmedTxs
        newTxs <- MempoolSyncer.updateTransactions(txs)
      } yield newTxs
    } else {
      Future.successful(NewTransactions(Map.empty))
    }

  def executePlugins(plugins: List[Plugin], newTxs: NewTransactions, utxoState: UtxoState): Future[Unit] =
    Future
      .sequence(
        plugins.map(_.execute(newTxs.txs, utxoState.addressByUtxo, utxoState.utxosByAddress))
      )
      .map(_ => ())

  def sync(plugins: List[Plugin]): Future[(ChainState, MempoolState)] =
    for {
      chainState <- ChainSyncer.getChainState
      fromHeight = chainState.getLastCachedBlock.map(_.height).getOrElse(0) + 1
      bestBlockHeight <- blockHttpClient.getBestBlockHeight
      pastHeights = chainState.findMissingIndexes.flatMap(Epoch.heightRangeForEpochIndex)
      _           = if (pastHeights.nonEmpty) logger.error(s"Going to index $pastHeights missing blocks")
      _           = if (bestBlockHeight > fromHeight) logger.info(s"Going to index blocks from $fromHeight to $bestBlockHeight")
      _           = if (bestBlockHeight == fromHeight) logger.info(s"Going to index block $bestBlockHeight")
      _               <- Source(pastHeights).concat(Source(fromHeight to bestBlockHeight)).via(indexingFlow).run()
      newChainState   <- ChainSyncer.getChainState
      newTransactions <- syncMempool(newChainState, bestBlockHeight)
      _               <- executePlugins(plugins, newTransactions, newChainState.utxoState)
      newMempoolState <- MempoolSyncer.getMempoolState
    } yield (newChainState, newMempoolState)

  def run(initialDelay: FiniteDuration, pollingInterval: FiniteDuration): Future[Done] =
    for {
      plugins <- Future.fromTry(Indexer.loadPlugins)
      _ = if (plugins.nonEmpty) logger.info(s"Plugins loaded: ${plugins.map(_.name).mkString(", ")}")
      chainState <- backend.getCachedState
      _          <- ChainSyncer.initialize(chainState)
      done       <- schedule(initialDelay, pollingInterval)(sync(plugins)).run()
    } yield done
}

object Indexer extends LazyLogging {

  def loadPlugins: Try[List[Plugin]] = Try(ServiceLoader.load(classOf[Plugin]).iterator().asScala.toList)

  def runWith(
    conf: ChainIndexerConf
  )(implicit ctx: ActorContext[Nothing]): Future[Done] = {
    implicit val system: ActorSystem[Nothing]                 = ctx.system
    implicit val protocol: ProtocolSettings                   = conf.protocol
    implicit val chainSyncerRef: ActorRef[ChainSyncerRequest] = ctx.spawn(new ChainSyncer().initialBehavior, "ChainSyncer")
    implicit val mempoolSyncerRef: ActorRef[MempoolSyncerRequest] =
      ctx.spawn(MempoolSyncer.behavior(MempoolState(Map.empty)), "MempoolSyncer")
    BlockHttpClient.withNodePoolBackend(conf).flatMap { blockHttpClient =>
      val indexer =
        conf.backendType match {
          case CassandraDb(parallelism) =>
            new Indexer(CassandraBackend(parallelism), blockHttpClient)
          case InMemoryDb =>
            new Indexer(new InMemoryBackend(), blockHttpClient)
        }
      indexer
        .run(0.seconds, 5.seconds)
        .andThen { case Failure(ex) =>
          logger.error(s"Shutting down due to unexpected error", ex)
          blockHttpClient.close().andThen { case _ => system.terminate() }
        }
    }
  }
}
