package org.ergoplatform.uexplorer.indexer.chain

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.{ActorAttributes, OverflowStrategy, SharedKillSwitch}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.ExeContext.Implicits
import org.ergoplatform.uexplorer.{BlockId, Height, ProtocolSettings, Resiliency, Storage}
import org.ergoplatform.uexplorer.cassandra.api.Backend
import org.ergoplatform.uexplorer.db.{BestBlockInserted, ForkInserted, FullBlock, Insertable}
import org.ergoplatform.uexplorer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.chain.ChainIndexer.{ChainSyncResult, ChainTip}
import org.ergoplatform.uexplorer.janusgraph.api.GraphBackend
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.storage.MvStorage
import scala.jdk.CollectionConverters.*
import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable.{ListMap, TreeSet}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.collection.concurrent
import scala.util.Try

class ChainIndexer(
  backendOpt: Option[Backend],
  graphBackendOpt: Option[GraphBackend],
  blockHttpClient: BlockHttpClient,
  blockIndexer: BlockIndexer
)(implicit s: ActorSystem[Nothing], ps: ProtocolSettings, killSwitch: SharedKillSwitch)
  extends LazyLogging {

  private val graphWriteFlow =
    Flow[BestBlockInserted]
      .buffer(100, OverflowStrategy.backpressure)
      .async
      .via(
        graphBackendOpt.fold(Flow.fromFunction[BestBlockInserted, BestBlockInserted](identity))(
          _.graphWriteFlow
        )
      )

  private val blockWriteFlow =
    Flow[BestBlockInserted]
      .buffer(100, OverflowStrategy.backpressure)
      .async
      .via(backendOpt.fold(Flow.fromFunction[BestBlockInserted, BestBlockInserted](identity))(_.blockWriteFlow))

  private val forkDeleteFlow: Flow[Insertable, BestBlockInserted, NotUsed] =
    Flow[Insertable]
      .mapAsync(1) {
        case ForkInserted(newBlocksInserted, supersededFork) =>
          backendOpt
            .fold(Future.successful(newBlocksInserted))(_.removeBlocksFromMainChain(supersededFork.keys))
            .map(_ => newBlocksInserted)
        case bb: BestBlockInserted =>
          if (bb.lightBlock.info.height % 100 == 0) {
            logger.info(s"Height ${bb.lightBlock.info.height}")
          }
          Future.successful(List(bb))
      }
      .mapConcat(identity)

  private def insertBlocks(blocks: List[ApiFullBlock]): Insertable =
    blocks match {
      case bestBlock :: Nil =>
        blockIndexer.addBestBlock(bestBlock).get
      case winningFork =>
        blockIndexer.addWinningFork(winningFork).get
    }

  private def onComplete(lastBlock: Option[BestBlockInserted]): ChainSyncResult = {
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
      blockIndexer.readableStorage,
      graphBackendOpt.map(_.graphTraversalSource)
    )
  }

  private def blockFlow(chainCache: concurrent.Map[BlockId, Height]): Flow[Height, List[ApiFullBlock], NotUsed] =
    Flow[Height]
      .mapAsync(1)(blockHttpClient.getBlockIdForHeight)
      .buffer(512, OverflowStrategy.backpressure)
      .mapAsync(1)(blockHttpClient.getBlockForId) // parallelism could be parameterized - low or big pressure on Node
      .buffer(1024, OverflowStrategy.backpressure)
      .mapAsync(1)(block => blockHttpClient.getBestBlockOrBranch(block, chainCache, List.empty))

  private def indexingSink(chainTip: ChainTip): Sink[Height, Future[ChainSyncResult]] =
    Flow[Height]
      .via(blockFlow(chainTip.toConcurrent))
      .map(insertBlocks)
      .async(ActorAttributes.IODispatcher.dispatcher)
      .via(forkDeleteFlow)
      .via(blockWriteFlow)
      .via(graphWriteFlow)
      .via(killSwitch.flow)
      .withAttributes(ActorAttributes.supervisionStrategy(Resiliency.decider))
      .toMat(Sink.lastOption[BestBlockInserted]) { case (_, lastBlockF) =>
        lastBlockF.map(onComplete)
      }

  def indexChain(chainTip: ChainTip): Future[ChainSyncResult] =
    for
      bestBlockHeight <- blockHttpClient.getBestBlockHeight
      fromHeight = chainTip.lastHeight.getOrElse(0) + 1
      _ = if (bestBlockHeight > fromHeight) logger.info(s"Going to index blocks from $fromHeight to $bestBlockHeight")
      _ = if (bestBlockHeight == fromHeight) logger.info(s"Going to index block $bestBlockHeight")
      syncResult <- Source(fromHeight to bestBlockHeight).runWith(indexingSink(chainTip))
    yield syncResult

  def fixChain(missingHeights: TreeSet[Height], chainTip: ChainTip): Future[ChainSyncResult] =
    Source(missingHeights)
      .runWith(indexingSink(chainTip))

}

object ChainIndexer {
  case class ChainTip(fromHighestToLowest: ListMap[BlockId, Height]) {
    def lastHeight: Option[Height] = fromHighestToLowest.headOption.map(_._2)
    def toConcurrent: concurrent.Map[BlockId, Height] =
      new ConcurrentHashMap[BlockId, Height]().asScala.addAll(fromHighestToLowest)
  }
  case class ChainSyncResult(
    lastBlock: Option[BestBlockInserted],
    storage: Storage,
    graphTraversalSource: Option[GraphTraversalSource]
  )
}
