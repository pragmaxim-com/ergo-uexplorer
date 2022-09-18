package org.ergoplatform.uexplorer.indexer

import akka.NotUsed
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.Attributes
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.explorer.settings.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.api.Backend
import org.ergoplatform.uexplorer.indexer.config.{ChainIndexerConf, ScyllaDb, UnknownDb}
import org.ergoplatform.uexplorer.indexer.http.{BlockHttpClient, MetadataHttpClient}
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor._
import org.ergoplatform.uexplorer.indexer.progress.{Epoch, ProgressMonitor}
import org.ergoplatform.uexplorer.indexer.scylla.ScyllaBackend
import sttp.client3.{HttpClientFutureBackend, SttpBackend, SttpBackendOptions}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Failure

class Indexer(
  backend: Backend,
  metaHttpClient: MetadataHttpClient[_],
  blockHttpClient: BlockHttpClient,
  progressMonitorRef: ActorRef[ProgressMonitorRequest]
)(implicit val s: ActorSystem[Nothing])
  extends AkkaStreamSupport
  with LazyLogging {

  implicit val timeout: Timeout = 3.seconds

  val blockToEpochFlow: Flow[FlatBlock, MaybeNewEpoch, NotUsed] =
    Flow[FlatBlock]
      .collect {
        case b if Epoch.heightAtFlushPoint(b.header.height) => Epoch.epochIndexForHeight(b.header.height) - 1
      }
      .mapAsync(1)(epochIndex => progressMonitorRef.ask(ref => GetFinishedEpoch(epochIndex, ref)))

  val indexingFlow: Flow[Int, Either[Int, Epoch], NotUsed] =
    Flow[Int]
      .via(blockHttpClient.blockCachingFlow(progressMonitorRef))
      .async
      .via(backend.blockWriteFlow)
      .via(blockToEpochFlow)
      .via(backend.epochWriteFlow)
      .withAttributes(supervisionStrategy(Resiliency.decider))

  def updateProgressFromDB(progressMonitorRef: ActorRef[ProgressMonitorRequest]): Future[ProgressState] =
    backend.getLastBlockInfoByEpochIndex.flatMap { lastBlockInfoByEpochIndex =>
      progressMonitorRef.ask(ref => UpdateEpochIndexes(lastBlockInfoByEpochIndex, ref))
    }

  def sync: Future[ProgressState] = {
    val bestBlockHeightF = metaHttpClient.getBestBlockHeight
    val progressF        = updateProgressFromDB(progressMonitorRef)
    bestBlockHeightF.flatMap { bestBlockHeight =>
      progressF.flatMap { progress =>
        val epochIndexesToLoad = progress.epochIndexesToDownload(bestBlockHeight)
        if (epochIndexesToLoad.size > 1) {
          logger.info(s"Initiating indexing of ${epochIndexesToLoad.size} epochs ...")
          Source(epochIndexesToLoad)
            .mapConcat(Epoch.heightRangeForEpochIndex)
            .via(indexingFlow)
            .run()
            .flatMap(_ => sync)
        } else {
          Future.successful(progress)
        }
      }
    }
  }

  def heightsToPollBlocksFor: Future[Vector[Int]] =
    progressMonitorRef
      .askWithStatus(ref => GetLastBlock(ref))
      .map(_.block.stats.height + 1)
      .flatMap(fromHeight => metaHttpClient.getBestBlockHeight.map(toHeight => (fromHeight to toHeight).toVector))

  def keepPolling: Future[ProgressState] =
    restartSource {
      Source
        .tick(0.seconds, 5.seconds, ())
        .mapAsync(1) { _ =>
          heightsToPollBlocksFor
            .flatMap { heights =>
              if (heights.nonEmpty) {
                logger.info(s"Going to index ${heights.size} block(s) starting at height ${heights.head}")
                Source(heights).via(indexingFlow).runWith(Sink.seq[Either[Int, Epoch]])
              } else {
                Future.successful(Seq.empty[Either[Int, Epoch]])
              }
            }
        }
        .withAttributes(Attributes.inputBuffer(0, 1))
    }.run().flatMap(_ => updateProgressFromDB(progressMonitorRef))

}

object Indexer extends LazyLogging {

  def runWith(conf: ChainIndexerConf)(implicit ctx: ActorContext[Nothing]): Future[ProgressState] = {
    implicit val system: ActorSystem[Nothing] = ctx.system
    implicit val protocol: ProtocolSettings   = conf.protocol
    implicit val futureSttpBackend: SttpBackend[Future, _] =
      HttpClientFutureBackend(SttpBackendOptions.connectionTimeout(5.seconds))

    val metadataClient     = MetadataHttpClient(conf)
    val blockHttpClient    = BlockHttpClient(metadataClient)
    val progressMonitorRef = ctx.spawn(new ProgressMonitor().initialBehavior, "ProgressMonitor")
    val indexer =
      conf.backendType match {
        case ScyllaDb =>
          new Indexer(ScyllaBackend(), metadataClient, blockHttpClient, progressMonitorRef)
        case UnknownDb =>
          throw new IllegalArgumentException(s"Unknown backend not supported yet.")
      }

    indexer.sync
      .flatMap {
        case state if state.isCacheEmpty =>
          Future.failed(new UnexpectedStateError("Syncing finished in unexpected cache state"))
        case _ =>
          logger.info(s"Initiating polling...")
          indexer.keepPolling
      }
      .andThen { case Failure(ex) =>
        metadataClient.close().flatMap(_ => blockHttpClient.close()).andThen { case _ => system.terminate() }
      }
  }
}
