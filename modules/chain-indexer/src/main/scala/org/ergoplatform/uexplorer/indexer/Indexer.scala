package org.ergoplatform.uexplorer.indexer

import akka.NotUsed
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.Attributes
import akka.stream.alpakka.cassandra.CassandraSessionSettings
import akka.stream.alpakka.cassandra.scaladsl.{CassandraSession, CassandraSessionRegistry}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.settings.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.api.{BlockBuilder, BlockWriter, EpochService}
import org.ergoplatform.uexplorer.indexer.config.{ChainIndexerConf, ScyllaBackend, UnknownBackend}
import org.ergoplatform.uexplorer.indexer.http.{BlockHttpClient, MetadataHttpClient}
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor.{GetLastBlock, ProgressMonitorRequest, ProgressState}
import org.ergoplatform.uexplorer.indexer.progress.{Epoch, ProgressMonitor}
import org.ergoplatform.uexplorer.indexer.scylla.{ScyllaBlockBuilder, ScyllaBlockWriter, ScyllaEpochService}
import sttp.client3.{HttpClientFutureBackend, SttpBackend, SttpBackendOptions}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.Failure

class Indexer(
  blockPersistence: BlockWriter,
  blockBuilder: BlockBuilder,
  epochService: EpochService,
  metaHttpClient: MetadataHttpClient[_],
  blockHttpClient: BlockHttpClient,
  progressMonitorRef: ActorRef[ProgressMonitorRequest]
)(implicit val s: ActorSystem[Nothing])
  extends AkkaStreamSupport
  with LazyLogging {

  implicit val timeout: Timeout = 3.seconds

  val indexingFlow: Flow[Int, Either[Int, Epoch], NotUsed] =
    Flow[Int]
      .via(blockHttpClient.blockResolvingFlow)
      .via(blockBuilder.blockBuildingFlow(blockHttpClient, progressMonitorRef))
      .async
      .via(blockPersistence.blockWriteFlow)
      .via(epochService.writeEpochFlow(progressMonitorRef))
      .withAttributes(supervisionStrategy(Resiliency.decider))

  def sync: Future[ProgressState] = {
    val bestBlockHeightF = metaHttpClient.getBestBlockHeight
    val progressF        = epochService.updateProgressFromDB(progressMonitorRef)
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
      .ask(ref => GetLastBlock(ref))
      .map(_.block.get.stats.height + 1)
      .flatMap(fromHeight => metaHttpClient.getBestBlockHeight.map(toHeight => (fromHeight to toHeight).toVector))

  def keepPolling: Future[ProgressState] =
    restartSource {
      Source
        .tick(0.seconds, 5.seconds, ())
        .mapAsync(1) { _ =>
          heightsToPollBlocksFor
            .flatMap { heights =>
              if (heights.nonEmpty) {
                logger.info(s"Going to index ${heights.size} blocks starting at height ${heights.head}")
                Source(heights).via(indexingFlow).runWith(Sink.seq[Either[Int, Epoch]])
              } else {
                Future.successful(Seq.empty[Either[Int, Epoch]])
              }
            }
        }
        .withAttributes(Attributes.inputBuffer(0, 1))
    }.run().flatMap(_ => epochService.updateProgressFromDB(progressMonitorRef))

}

object Indexer extends LazyLogging {

  def runWith(conf: ChainIndexerConf)(implicit ctx: ActorContext[Nothing]): Future[ProgressState] = {
    implicit val system: ActorSystem[Nothing] = ctx.system
    implicit val protocol: ProtocolSettings   = conf.protocol
    implicit val futureSttpBackend: SttpBackend[Future, _] =
      HttpClientFutureBackend(SttpBackendOptions.connectionTimeout(5.seconds))
    val metadataClient = new MetadataHttpClient(conf.peerAddressToPollFrom, conf.nodeAddressToInitFrom)
    val pollingClient  = BlockHttpClient(metadataClient)
    val indexer =
      conf.backendType match {
        case ScyllaBackend =>
          implicit val cassandraSession: CassandraSession =
            CassandraSessionRegistry.get(system).sessionFor(CassandraSessionSettings())
          implicit val cqlSession: CqlSession = Await.result(cassandraSession.underlying(), 5.seconds)
          new Indexer(
            new ScyllaBlockWriter,
            new ScyllaBlockBuilder,
            new ScyllaEpochService(),
            metadataClient,
            pollingClient,
            ctx.spawn(new ProgressMonitor().initialBehavior, "ProgressMonitor")
          )
        case UnknownBackend =>
          throw new IllegalArgumentException(s"Unknown backend not supported yet.")
      }

    indexer.sync
      .andThen { case Failure(ex) =>
        logger.error(
          s"Initial sync failed, there is no restart as DB could be corrupted if you find SIGKILL in scylla logs due to OOM error",
          ex
        )
        metadataClient.close().flatMap(_ => pollingClient.close()).andThen { case _ => system.terminate() }
      }
      .flatMap { _ =>
        logger.info(s"Initiating polling...")
        indexer.keepPolling
      }
  }
}
