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
import org.ergoplatform.uexplorer.db.{
  BestBlockInserted,
  Block,
  BlockInfo,
  BlockInfoBuilder,
  ForkInserted,
  FullBlock,
  FullBlockBuilder,
  Insertable
}
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

  /** Genesis block has no parent so we assert that any block either has its parent cached or its a first block */
  private def getParentOrFail(
    apiBlock: ApiFullBlock,
    chainCache: concurrent.Map[BlockId, BlockInfo]
  ): Try[Option[BlockInfo]] = {
    def fail =
      Failure(
        new IllegalStateException(
          s"Block ${apiBlock.header.id} at height ${apiBlock.header.height} has missing parent ${apiBlock.header.parentId}"
        )
      )

    if (apiBlock.header.height == 1)
      Try(Option.empty)
    else
      chainCache.get(apiBlock.header.parentId).fold(fail)(parent => Try(Option(parent)))
  }

  private def buildBlock(apiBlock: ApiFullBlock, chainCache: concurrent.Map[BlockId, BlockInfo]): Try[Block] =
    for {
      parentOpt <- getParentOrFail(apiBlock, chainCache)
      blockInfo <- BlockInfoBuilder(apiBlock, parentOpt)
      fbOpt     <- if (backendOpt.isDefined) FullBlockBuilder(apiBlock, parentOpt).map(Some(_)) else Try(None)
    } yield Block(apiBlock.header.id, blockInfo, fbOpt, apiBlock.transactions.transactions)

  def getBestBlockOrBranch(
    block: ApiFullBlock,
    chainCache: concurrent.Map[BlockId, BlockInfo], // does not have to be concurrent unless akka-stream parallelism > 1
    acc: List[Block]
  ): Future[List[Block]] =
    chainCache.get(block.header.parentId).exists(_.height == block.header.height - 1) match {
      case blockCached if blockCached =>
        Future.fromTry(buildBlock(block, chainCache)).map { case b @ Block(_, newBlockInfo, _, _) =>
          chainCache.put(block.header.id, newBlockInfo)
          b :: acc
        }
      case _ if block.header.height == 1 =>
        Future.fromTry(buildBlock(block, chainCache)).map { case b @ Block(_, newBlockInfo, _, _) =>
          chainCache.put(block.header.id, newBlockInfo)
          b :: acc
        }
      case _ =>
        logger.info(s"Encountered fork at height ${block.header.height} and block ${block.header.id}")
        for {
          newBlock <- Future.fromTry(buildBlock(block, chainCache))
          b        <- blockHttpClient.getBlockForId(block.header.parentId)
          bbOrB    <- getBestBlockOrBranch(b, chainCache, newBlock :: acc)
        } yield bbOrB
    }

  private def insertBlocks(blocks: List[Block]): Insertable =
    blocks match {
      case bestBlock :: Nil =>
        blockIndexer.addBestBlock(bestBlock).get
      case winningFork =>
        blockIndexer.addWinningFork(winningFork).get
    }

  private def blockFlow(
    chainCache: concurrent.Map[BlockId, BlockInfo]
  ): Flow[Height, List[Block], NotUsed] =
    Flow[Height]
      .mapAsync(1)(blockHttpClient.getBlockIdForHeight)
      .buffer(512, OverflowStrategy.backpressure)
      .mapAsync(1)(blockHttpClient.getBlockForId) // parallelism could be parameterized - low or big pressure on Node
      .buffer(1024, OverflowStrategy.backpressure)
      .mapAsync(1)(block => getBestBlockOrBranch(block, chainCache, List.empty))

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
