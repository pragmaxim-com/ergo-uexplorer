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
import org.ergoplatform.uexplorer.chain.{ChainLinker, ChainTip}
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.chain.ChainIndexer.ChainSyncResult
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

  private val graphPersistenceFlow =
    Flow[BestBlockInserted]
      .buffer(100, OverflowStrategy.backpressure)
      .async
      .via(
        graphBackendOpt.fold(Flow.fromFunction[BestBlockInserted, BestBlockInserted](identity))(
          _.graphWriteFlow
        )
      )

  private val backendPersistence =
    Flow[BestBlockInserted]
      .buffer(100, OverflowStrategy.backpressure)
      .async
      .via(backendOpt.fold(Flow.fromFunction[BestBlockInserted, BestBlockInserted](identity))(_.blockWriteFlow))

  private val httpFlow: Flow[Height, ApiFullBlock, NotUsed] =
    Flow[Height]
      .mapAsync(1)(blockHttpClient.getBlockIdForHeight)
      .buffer(512, OverflowStrategy.backpressure)
      .mapAsync(1)(blockHttpClient.getBlockForId) // parallelism could be parameterized - low or big pressure on Node
      .buffer(8192, OverflowStrategy.backpressure)

  private val processingFlow: Flow[ApiFullBlock, BlockWithOutputs, NotUsed] =
    Flow[ApiFullBlock]
      .map(RewardCalculator(_).get)
      .async
      .buffer(8192, OverflowStrategy.backpressure)
      .map(OutputBuilder(_).get)
      .async
      .addAttributes(Attributes.inputBuffer(1, 8)) // contract processing (sigma, base58)
      .buffer(8192, OverflowStrategy.backpressure)

  private val insertBranchFlow: Flow[List[LinkedBlock], BestBlockInserted, NotUsed] =
    Flow
      .apply[List[LinkedBlock]]
      .flatMapConcat {
        case bestBlock :: Nil =>
          if (bestBlock.info.height % 100 == 0) {
            logger.info(s"Height ${bestBlock.info.height}")
          }
          Source.single(bestBlock).via(blockIndexer.insertBlockFlow)
        case winningFork =>
          blockIndexer
            .rollbackFork(winningFork, backendOpt)
            .fold(
              Source.failed[BestBlockInserted],
              branch => Source(branch).via(blockIndexer.insertBlockFlow)
            )
      }

  private def indexingSink(chainLinker: ChainLinker): Sink[Height, Future[ChainSyncResult]] =
    Flow[Height]
      .via(httpFlow)
      .via(processingFlow)
      .mapAsync(1)(chainLinker.linkChildToAncestors())
      .buffer(8192, OverflowStrategy.backpressure)
      .via(insertBranchFlow)
      .via(backendPersistence)
      .via(graphPersistenceFlow)
      .via(killSwitch.flow)
      .withAttributes(ActorAttributes.supervisionStrategy(Resiliency.decider))
      .toMat(Sink.lastOption[BestBlockInserted]) { case (_, lastBlockF) =>
        lastBlockF.map(onComplete)
      }

  private def onComplete(lastBlock: Option[BestBlockInserted]): ChainSyncResult = {
    blockIndexer.finishIndexing
    ChainSyncResult(
      lastBlock,
      blockIndexer.readableStorage,
      graphBackendOpt.map(_.graphTraversalSource)
    )
  }

  def indexChain(chainTip: ChainTip): Future[ChainSyncResult] =
    for
      bestBlockHeight <- blockHttpClient.getBestBlockHeight
      fromHeight = blockIndexer.readableStorage.getLastHeight.getOrElse(0) + 1
      _ = if (bestBlockHeight > fromHeight) logger.info(s"Going to index blocks from $fromHeight to $bestBlockHeight")
      _ = if (bestBlockHeight == fromHeight) logger.info(s"Going to index block $bestBlockHeight")
      chainLinker = new ChainLinker(blockHttpClient.getBlockForId, chainTip)
      syncResult <- Source(fromHeight to bestBlockHeight).runWith(indexingSink(chainLinker))
    yield syncResult

  def fixChain(missingHeights: TreeSet[Height], chainTip: ChainTip): Future[ChainSyncResult] =
    Source(missingHeights)
      .runWith(indexingSink(new ChainLinker(blockHttpClient.getBlockForId, chainTip)))

}

object ChainIndexer {
  case class ChainSyncResult(
    lastBlock: Option[BestBlockInserted],
    storage: Storage,
    graphTraversalSource: Option[GraphTraversalSource]
  )
}
