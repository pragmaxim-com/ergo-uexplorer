package org.ergoplatform.uexplorer.indexer

import akka.{Done, NotUsed}
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.explorer.settings.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.api.{Backend, InMemoryBackend}
import org.ergoplatform.uexplorer.indexer.config.{ChainIndexerConf, InMemoryDb, CassandraDb}
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor._
import org.ergoplatform.uexplorer.indexer.progress.{Epoch, ProgressMonitor, ProgressState}
import org.ergoplatform.uexplorer.indexer.cassandra.CassandraBackend

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Failure

class Indexer(backend: Backend, blockHttpClient: BlockHttpClient)(implicit
  val s: ActorSystem[Nothing],
  progressMonitorRef: ActorRef[MonitorRequest]
) extends AkkaStreamSupport
  with LazyLogging {

  val blockToEpochFlow: Flow[FlatBlock, (FlatBlock, Option[MaybeNewEpoch]), NotUsed] =
    Flow[FlatBlock]
      .mapAsync(1) {
        case block if Epoch.heightAtFlushPoint(block.header.height) =>
          ProgressMonitor
            .finishEpoch(Epoch.epochIndexForHeight(block.header.height) - 1)
            .map(e => block -> Option(e))
        case block =>
          Future.successful(block -> Option.empty)
      }

  val indexingFlow: Flow[Int, (FlatBlock, Option[MaybeNewEpoch]), NotUsed] =
    Flow[Int]
      .via(blockHttpClient.blockCachingFlow)
      .async
      .via(backend.blockWriteFlow)
      .via(blockToEpochFlow)
      .via(backend.epochWriteFlow)
      .withAttributes(supervisionStrategy(Resiliency.decider))

  def sync: Future[ProgressState] =
    for {
      progress <- ProgressMonitor.getChainState
      fromHeight = progress.getLastCachedBlock.map(_.stats.height).getOrElse(0) + 1
      toHeight <- blockHttpClient.getBestBlockHeight
      pastHeights = progress.findMissingIndexes.flatMap(Epoch.heightRangeForEpochIndex)
      _           = if (pastHeights.nonEmpty) logger.error(s"Going to index $pastHeights missing blocks")
      _           = if (toHeight > fromHeight) logger.info(s"Going to index blocks from $fromHeight to $toHeight")
      _           = if (toHeight == fromHeight) logger.info(s"Going to index block $toHeight")
      _           <- Source(pastHeights).concat(Source(fromHeight to toHeight)).via(indexingFlow).run()
      newProgress <- ProgressMonitor.getChainState
    } yield newProgress

  def run(pollingInterval: FiniteDuration): Future[Done] =
    for {
      lastBlockInfoByEpochIndex <- backend.getLastBlockInfoByEpochIndex
      progress                  <- ProgressMonitor.updateState(lastBlockInfoByEpochIndex)
      _ = logger.info(s"Initiating indexing at $progress")
      newProgress <- schedule(pollingInterval)(sync).run()
    } yield newProgress
}

object Indexer extends LazyLogging {

  def runWith(
    conf: ChainIndexerConf
  )(implicit ctx: ActorContext[Nothing]): Future[Done] = {
    implicit val system: ActorSystem[Nothing]         = ctx.system
    implicit val protocol: ProtocolSettings           = conf.protocol
    implicit val monitorRef: ActorRef[MonitorRequest] = ctx.spawn(new ProgressMonitor().initialBehavior, "Monitor")
    val blockHttpClient                               = BlockHttpClient(conf)
    val indexer =
      conf.backendType match {
        case CassandraDb =>
          new Indexer(CassandraBackend(), blockHttpClient)
        case InMemoryDb =>
          new Indexer(new InMemoryBackend(), blockHttpClient)
      }
    indexer
      .run(5.seconds)
      .andThen { case Failure(ex) =>
        logger.error(s"Shutting down due to unexpected error", ex)
        blockHttpClient.close().andThen { case _ => system.terminate() }
      }

  }
}
