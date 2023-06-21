package org.ergoplatform.uexplorer.indexer.chain

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.{ActorAttributes, Attributes, OverflowStrategy, SharedKillSwitch}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.ExeContext.Implicits
import org.ergoplatform.uexplorer.{BlockId, Height, ProtocolSettings, Resiliency, Storage}
import org.ergoplatform.uexplorer.cassandra.api.Backend
import org.ergoplatform.uexplorer.db.*
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
import scala.util.{Failure, Try}

class ChainIndexer(
  backendOpt: Option[Backend],
  graphBackendOpt: Option[GraphBackend],
  blockHttpClient: BlockHttpClient,
  blockIndexer: BlockIndexer
)(implicit s: ActorSystem[Nothing], ps: ProtocolSettings, killSwitch: SharedKillSwitch)
  extends LazyLogging {

  implicit private val enc: ErgoAddressEncoder = ps.addressEncoder

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
          if (bb.lightBlock.blockInfo.height % 100 == 0) {
            logger.info(s"Height ${bb.lightBlock.blockInfo.height}")
          }
          Future.successful(List(bb))
      }
      .mapConcat(identity)

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

  private def insertBlocks(blocks: List[LinkedBlock]): Insertable =
    blocks match {
      case bestBlock :: Nil =>
        blockIndexer.addBestBlock(bestBlock).get
      case winningFork =>
        blockIndexer.addWinningFork(winningFork).get
    }

  private def blockFlow(
    chainLinker: ChainLinker
  ): Flow[Height, List[LinkedBlock], NotUsed] =
    Flow[Height]
      .mapAsync(1)(blockHttpClient.getBlockIdForHeight)
      .buffer(512, OverflowStrategy.backpressure)
      .mapAsync(1)(blockHttpClient.getBlockForId) // parallelism could be parameterized - low or big pressure on Node
      .buffer(8192, OverflowStrategy.backpressure)
      .map(RewardCalculator(_).get)
      .async
      .buffer(8192, OverflowStrategy.backpressure)
      .map(OutputParser(_).get)
      .async
      .addAttributes(Attributes.inputBuffer(1, 8)) // contract processing (sigma, base58)
      .buffer(8192, OverflowStrategy.backpressure)
      .mapAsync(1)(block => chainLinker.linkChildToAncestors(block, List.empty))

  private def indexingSink(chainLinker: ChainLinker): Sink[Height, Future[ChainSyncResult]] =
    Flow[Height]
      .via(blockFlow(chainLinker))
      .buffer(8192, OverflowStrategy.backpressure)
      .map(insertBlocks)
      .async(ActorAttributes.IODispatcher.dispatcher, 8)
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
      chainLinker = new ChainLinker(blockHttpClient.getBlockForId, chainTip.toConcurrent)
      syncResult <- Source(fromHeight to bestBlockHeight).runWith(indexingSink(chainLinker))
    yield syncResult

  def fixChain(missingHeights: TreeSet[Height], chainTip: ChainTip): Future[ChainSyncResult] =
    Source(missingHeights)
      .runWith(indexingSink(new ChainLinker(blockHttpClient.getBlockForId, chainTip.toConcurrent)))

}

object ChainIndexer {
  case class ChainTip(fromHighestToLowest: ListMap[BlockId, BlockInfo]) {
    def lastHeight: Option[Height] = fromHighestToLowest.headOption.map(_._2.height)
    def toConcurrent: concurrent.Map[BlockId, BlockInfo] =
      new ConcurrentHashMap[BlockId, BlockInfo]().asScala.addAll(fromHighestToLowest)
  }
  case class ChainSyncResult(
    lastBlock: Option[BestBlockInserted],
    storage: Storage,
    graphTraversalSource: Option[GraphTraversalSource]
  )
}
