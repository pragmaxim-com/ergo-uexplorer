package org.ergoplatform.uexplorer.indexer.chain

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.{ActorAttributes, OverflowStrategy, SharedKillSwitch}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.ExeContext.Implicits
import org.ergoplatform.uexplorer.{Height, ProtocolSettings, Resiliency, Storage}
import org.ergoplatform.uexplorer.cassandra.api.Backend
import org.ergoplatform.uexplorer.db.{BestBlockInserted, ForkInserted, FullBlock, Insertable}
import org.ergoplatform.uexplorer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.chain.ChainIndexer.ChainSyncResult
import org.ergoplatform.uexplorer.janusgraph.api.GraphBackend
import org.ergoplatform.uexplorer.storage.MvStorage

import scala.concurrent.blocking
import scala.collection.immutable.TreeSet
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ChainIndexer(
  backendOpt: Option[Backend],
  graphBackendOpt: Option[GraphBackend],
  blockHttpClient: BlockHttpClient,
  blockIndexer: BlockIndexer
)(implicit s: ActorSystem[Nothing], ps: ProtocolSettings, killSwitch: SharedKillSwitch)
  extends LazyLogging {

  private def forkDeleteFlow(parallelism: Int): Flow[Insertable, BestBlockInserted, NotUsed] =
    Flow[Insertable]
      .mapAsync(parallelism) {
        case ForkInserted(newBlocksInserted, supersededFork) =>
          backendOpt
            .fold(Future.successful(newBlocksInserted))(_.removeBlocksFromMainChain(supersededFork.keys))
            .map(_ => newBlocksInserted)
        case bb: BestBlockInserted =>
          Future.successful(List(bb))
      }
      .mapConcat(identity)

  private val indexingSink: Sink[Height, Future[ChainSyncResult]] =
    Flow[Height]
      .via(blockHttpClient.blockFlow)
      .mapAsync(1) { block =>
        blockHttpClient
          .getBestBlockOrBranch(block, blockIndexer.readableStorage.containsBlock, List.empty)(Implicits.trampoline)
          .map {
            case bestBlock :: Nil =>
              blockIndexer.addBestBlock(bestBlock).get
            case winningFork =>
              blockIndexer.addWinningFork(winningFork).get
          }(Implicits.trampoline)
      }
      .via(forkDeleteFlow(1))
      .async
      .buffer(100, OverflowStrategy.backpressure)
      .via(backendOpt.fold(Flow.fromFunction[BestBlockInserted, BestBlockInserted](identity))(_.blockWriteFlow))
      .wireTap { bb =>
        if (bb.lightBlock.info.height % 100 == 0) {
          logger.info(s"Height ${bb.lightBlock.info.height}")
        }
      }
      .async
      .buffer(100, OverflowStrategy.backpressure)
      .via(
        graphBackendOpt.fold(Flow.fromFunction[BestBlockInserted, BestBlockInserted](identity))(
          _.graphWriteFlow
        )
      )
      .via(killSwitch.flow)
      .withAttributes(ActorAttributes.supervisionStrategy(Resiliency.decider))
      .toMat(Sink.lastOption[BestBlockInserted]) { case (_, lastBlockF) =>
        lastBlockF.map { lastBlock =>
          val storage = blockIndexer.readableStorage
          storage.writeReport.recover { case ex =>
            logger.error("Failed to generate report", ex)
          }
          blockIndexer
            .compact(indexing = false)
            .recover { case ex =>
              logger.error("Compaction failed", ex)
            }
          ChainSyncResult(
            lastBlock,
            storage,
            graphBackendOpt.map(_.graphTraversalSource)
          )
        }
      }

  def indexChain: Future[ChainSyncResult] =
    for
      bestBlockHeight <- blockHttpClient.getBestBlockHeight
      fromHeight = blockIndexer.readableStorage.getLastHeight.getOrElse(0) + 1
      _ = if (bestBlockHeight > fromHeight) logger.info(s"Going to index blocks from $fromHeight to $bestBlockHeight")
      _ = if (bestBlockHeight == fromHeight) logger.info(s"Going to index block $bestBlockHeight")
      syncResult <- Source(fromHeight to bestBlockHeight).runWith(indexingSink)
    yield syncResult

  def fixChain(missingHeights: TreeSet[Height]): Future[ChainSyncResult] =
    Source(missingHeights)
      .runWith(indexingSink)

}

object ChainIndexer {
  case class ChainSyncResult(
    lastBlock: Option[BestBlockInserted],
    storage: Storage,
    graphTraversalSource: Option[GraphTraversalSource]
  )
}
