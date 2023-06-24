package org.ergoplatform.uexplorer.indexer.chain

import akka.{Done, NotUsed}
import akka.actor.typed.ActorSystem
import akka.stream.{ActorAttributes, Attributes, IOResult, OverflowStrategy, SharedKillSwitch}
import akka.stream.scaladsl.{Compression, FileIO, Flow, Framing, Sink, Source}
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.ExeContext.Implicits
import org.ergoplatform.uexplorer.{BlockId, Height, ProtocolSettings, Resiliency, Storage}
import org.ergoplatform.uexplorer.cassandra.api.Backend
import org.ergoplatform.uexplorer.chain.{ChainLinker, ChainTip}
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.http.{BlockHttpClient, Codecs}
import org.ergoplatform.uexplorer.indexer.chain.ChainIndexer.*
import org.ergoplatform.uexplorer.janusgraph.api.GraphBackend
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.storage.MvStorage
import sttp.client3.circe.asJson

import java.nio.file.{Path, Paths}
import scala.jdk.CollectionConverters.*
import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable.{ListMap, TreeSet}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.collection.concurrent
import scala.util.{Failure, Try}

class ChainIndexer(
  bench: Boolean,
  backendOpt: Option[Backend],
  graphBackendOpt: Option[GraphBackend],
  blockHttpClient: BlockHttpClient,
  blockIndexer: BlockIndexer
)(implicit s: ActorSystem[Nothing], ps: ProtocolSettings, killSwitch: SharedKillSwitch)
  extends LazyLogging {

  implicit private val enc: ErgoAddressEncoder = ps.addressEncoder

  private val blockForIdFlow: Flow[BlockId, ApiFullBlock, NotUsed] =
    Flow[BlockId]
      .mapAsync(1)(blockHttpClient.getBlockForId) // parallelism could be parameterized - low or big pressure on Node
      .buffer(4096, OverflowStrategy.backpressure)

  private val processingFlow: Flow[ApiFullBlock, BlockWithOutputs, NotUsed] =
    Flow[ApiFullBlock]
      .map(RewardCalculator(_).get)
      .async
      .buffer(4096, OverflowStrategy.backpressure)
      .map(OutputBuilder(_).get)
      .async
      .addAttributes(Attributes.inputBuffer(1, 8)) // contract processing (sigma, base58)
      .buffer(4096, OverflowStrategy.backpressure)

  private val insertBranchFlow: Flow[List[LinkedBlock], BestBlockInserted, NotUsed] =
    Flow
      .apply[List[LinkedBlock]]
      .flatMapConcat {
        case bestBlock :: Nil =>
          Source.single(bestBlock).via(blockIndexer.insertBlockFlow)
        case winningFork =>
          blockIndexer
            .rollbackFork(winningFork, backendOpt)
            .fold(
              Source.failed[BestBlockInserted],
              branch => Source(branch).via(blockIndexer.insertBlockFlow)
            )
      }

  private def indexingSink(chainLinker: ChainLinker): Sink[ApiFullBlock, Future[ChainSyncResult]] =
    Flow[ApiFullBlock]
      .via(processingFlow)
      .mapAsync(1)(chainLinker.linkChildToAncestors())
      .buffer(4096, OverflowStrategy.backpressure)
      .via(insertBranchFlow)
      .via(backendPersistence)
      .via(graphPersistenceFlow)
      .via(killSwitch.flow)
      .withAttributes(ActorAttributes.supervisionStrategy(Resiliency.decider))
      .toMat(Sink.lastOption[BestBlockInserted]) { case (_, lastBlockF) =>
        lastBlockF.map(onComplete)
      }

  private val backendPersistence =
    Flow[BestBlockInserted]
      .buffer(100, OverflowStrategy.backpressure)
      .async
      .via(backendOpt.fold(Flow.fromFunction[BestBlockInserted, BestBlockInserted](identity))(_.blockWriteFlow))

  private val graphPersistenceFlow =
    Flow[BestBlockInserted]
      .buffer(100, OverflowStrategy.backpressure)
      .async
      .via(
        graphBackendOpt.fold(Flow.fromFunction[BestBlockInserted, BestBlockInserted](identity))(
          _.graphWriteFlow
        )
      )

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
      source = if (bench) blockSourceFromFS(benchPath) else blockIdSource(blockHttpClient, fromHeight).via(blockForIdFlow)
      syncResult <- source.runWith(indexingSink(chainLinker))
    yield syncResult

  def fixChain(missingHeights: TreeSet[Height], chainTip: ChainTip): Future[ChainSyncResult] =
    Source(missingHeights)
      .mapAsync(1)(blockHttpClient.getBlockIdForHeight)
      .via(blockForIdFlow)
      .runWith(indexingSink(new ChainLinker(blockHttpClient.getBlockForId, chainTip)))

}

object ChainIndexer extends Codecs with LazyLogging {
  private val benchPath = Paths.get(System.getProperty("user.home"), ".ergo-uexplorer", "ergo-chain.lines.gz")
  case class ChainSyncResult(
    lastBlock: Option[BestBlockInserted],
    storage: Storage,
    graphTraversalSource: Option[GraphTraversalSource]
  )

  protected[indexer] def blockSourceFromFS(path: Path): Source[ApiFullBlock, NotUsed] = {
    import io.circe.parser.decode
    logger.info(s"Running benchmark, reading all blocks from filesystem")
    FileIO
      .fromPath(path)
      .via(Compression.gunzip())
      .via(Framing.delimiter(ByteString("\n"), 512 * 1000))
      .map(_.utf8String)
      .map(s => decode[ApiFullBlock](s).toTry.get)
      .mapMaterializedValue(_ => NotUsed)
  }

  protected[indexer] def blockIdSource(blockHttpClient: BlockHttpClient, fromHeight: Int): Source[BlockId, NotUsed] =
    Source
      .unfoldAsync(fromHeight) { offset =>
        blockHttpClient.getBlockIdsByOffset(offset, 100).map {
          case blockIds if blockIds.nonEmpty =>
            logger.info(s"Height $offset")
            Some((offset + blockIds.size, blockIds))
          case _ =>
            None
        }
      }
      .buffer(10, OverflowStrategy.backpressure)
      .mapConcat(identity)

}
