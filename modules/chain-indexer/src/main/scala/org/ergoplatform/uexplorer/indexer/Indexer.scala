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

class Indexer(backend: Backend, blockHttpClient: BlockHttpClient, snapshotManager: UtxoSnapshotManager)(implicit
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

  private val indexingFlow: Flow[Int, NewEpochCreated, NotUsed] =
    Flow[Int]
      .via(blockHttpClient.blockCachingFlow)
      .async
      .via(backend.blockWriteFlow)
      .via(blockToEpochFlow)
      .via(backend.epochsWriteFlow)
      .mapConcat(_._2.collect { case e @ NewEpochCreated(_) => e }.toList)
      .withAttributes(supervisionStrategy(Resiliency.decider))

  def syncMempool(chainState: ChainState, bestBlockHeight: Int): Future[MempoolStateChanges] =
    if (chainState.blockBuffer.byHeight.lastOption.map(_._1).exists(_ >= bestBlockHeight)) {
      for {
        txs          <- blockHttpClient.getUnconfirmedTxs
        stateChanges <- MempoolSyncer.updateTransactions(txs)
      } yield stateChanges
    } else {
      Future.successful(MempoolStateChanges(List.empty))
    }

  def executePlugins(plugins: List[Plugin], chainState: ChainState, stateChanges: MempoolStateChanges): Future[Unit] =
    Future
      .sequence(
        stateChanges.stateTransitionByTx.flatMap { case (newTx, poolTxs) =>
          val (inputs, outputs) =
            poolTxs.values.foldLeft((ArraySeq.newBuilder[BoxId], ArraySeq.newBuilder[(BoxId, Address, Long)])) {
              case ((iAcc, oAcc), tx) =>
                iAcc.addAll(tx.inputs.map(_.boxId)) -> oAcc.addAll(tx.outputs.map(o => (o.boxId, o.address, o.value)))
            }
          val utxoStateWoPool = chainState.utxoStateWithMergedBoxes
          val utxoStateWithPool =
            utxoStateWoPool.mergeBoxes(List((inputs.result(), outputs.result())).iterator)
          plugins.map(
            _.execute(
              newTx,
              UtxoStateWithoutPool(utxoStateWoPool.addressByUtxo, utxoStateWoPool.utxosByAddress),
              UtxoStateWithPool(utxoStateWithPool.addressByUtxo, utxoStateWithPool.utxosByAddress)
            )
          )
        }
      )
      .map(_ => ())

  def verifyStateIntegrity(chainState: ChainState): Future[Done] = {
    val pastHeights = chainState.findMissingIndexes.flatMap(Epoch.heightRangeForEpochIndex)
    if (pastHeights.nonEmpty) {
      logger.error(s"Going to index missing blocks at heights : ${pastHeights.mkString(", ")}")
      Source(pastHeights)
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

  def loadChainState: Future[ChainState] =
    backend.loadBlockInfoByEpochIndex
      .flatMap { blockInfoByEpochIndex =>
        snapshotManager.getLatestSnapshotByIndex
          .flatMap {
            _.collect {
              case snapshot if snapshot.epochIndex == blockInfoByEpochIndex.lastKey => Future.successful(snapshot.utxoState)
            }.getOrElse(backend.loadUtxoState(blockInfoByEpochIndex.keysIterator))
          }
          .map(utxoState => ChainState.load(blockInfoByEpochIndex, utxoState))
      }

  def syncChain(bestBlockHeight: Int): Future[ChainState] = for {
    chainState <- ChainSyncer.getChainState
    fromHeight = chainState.getLastCachedBlock.map(_.height).getOrElse(0) + 1
    _          = if (bestBlockHeight > fromHeight) logger.info(s"Going to index blocks from $fromHeight to $bestBlockHeight")
    _          = if (bestBlockHeight == fromHeight) logger.info(s"Going to index block $bestBlockHeight")
    newEpochOpt   <- Source(fromHeight to bestBlockHeight).via(indexingFlow).runWith(Sink.lastOption)
    newChainState <- ChainSyncer.getChainState
    _ = newEpochOpt.foreach(newEpoch =>
          snapshotManager.saveSnapshot(UtxoSnapshot.Deserialized(newEpoch.epoch.index, newChainState.utxoState))
        )
  } yield newChainState

  def periodicSync(plugins: List[Plugin]): Future[(ChainState, MempoolStateChanges)] =
    for {
      bestBlockHeight <- blockHttpClient.getBestBlockHeight
      chainState      <- syncChain(bestBlockHeight)
      stateChanges    <- syncMempool(chainState, bestBlockHeight)
      _               <- executePlugins(plugins, chainState, stateChanges)
    } yield (chainState, stateChanges)

  def run(initialDelay: FiniteDuration, pollingInterval: FiniteDuration): Future[Done] =
    for {
      plugins <- Future.fromTry(Indexer.loadPlugins)
      _       <- Future.fromTry(Try(plugins.map(_.init.get)))
      _ = if (plugins.nonEmpty) logger.info(s"Plugins loaded: ${plugins.map(_.name).mkString(", ")}")
      chainState <- loadChainState
      _          <- ChainSyncer.initialize(chainState)
      _          <- verifyStateIntegrity(chainState)
      done       <- schedule(initialDelay, pollingInterval)(periodicSync(plugins)).run()
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
      ctx.spawn(MempoolSyncer.behavior(MempoolState(ListMap.empty)), "MempoolSyncer")
    BlockHttpClient.withNodePoolBackend(conf).flatMap { blockHttpClient =>
      val snapshotManager = new UtxoSnapshotManager()
      val indexer =
        conf.backendType match {
          case CassandraDb(parallelism) =>
            new Indexer(CassandraBackend(parallelism), blockHttpClient, snapshotManager)
          case InMemoryDb =>
            new Indexer(new InMemoryBackend(), blockHttpClient, snapshotManager)
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
