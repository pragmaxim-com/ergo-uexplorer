package org.ergoplatform.uexplorer.indexer.chain

import akka.{Done, NotUsed}
import akka.actor.typed.ActorSystem
import akka.stream.{ActorAttributes, Attributes, IOResult, OverflowStrategy, SharedKillSwitch}
import akka.stream.scaladsl.{Compression, FileIO, Flow, Framing, Keep, Sink, Source}
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.ExeContext.Implicits
import org.ergoplatform.uexplorer.{BlockId, Height, ProtocolSettings, Resiliency, Storage}
import org.ergoplatform.uexplorer.chain.{ChainLinker, ChainTip}
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.http.{BlockHttpClient, Codecs}
import org.ergoplatform.uexplorer.indexer.chain.StreamExecutor.*
import org.ergoplatform.uexplorer.indexer.db.Backend
import org.ergoplatform.uexplorer.janusgraph.api.GraphBackend
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.storage.MvStorage

import java.nio.file.{Path, Paths}
import scala.jdk.CollectionConverters.*
import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable.{ListMap, TreeSet}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.collection.concurrent
import scala.util.{Failure, Success, Try}

class StreamExecutor(
  bench: Boolean,
  blockHttpClient: BlockHttpClient,
  blockReader: BlockReader,
  blockWriter: BlockWriter,
  storage: Storage
)(implicit s: ActorSystem[Nothing], ps: ProtocolSettings, killSwitch: SharedKillSwitch)
  extends LazyLogging {

  private def blockIndexingSink(chainLinker: ChainLinker): Sink[ApiFullBlock, Future[ChainSyncResult]] =
    Flow[ApiFullBlock]
      .via(BlockProcessor.processingFlow(chainLinker))
      .viaMat(blockWriter.insertBranchFlow)(Keep.right)
      .via(killSwitch.flow)
      .withAttributes(ActorAttributes.supervisionStrategy(Resiliency.decider))
      .to(Sink.ignore)

  def indexNewBlocks: Future[ChainSyncResult] =
    for
      bestBlockHeight <- blockHttpClient.getBestBlockHeight
      fromHeight = storage.getLastHeight.getOrElse(0) + 1
      _ = if (bestBlockHeight > fromHeight) logger.info(s"Going to index blocks from $fromHeight to $bestBlockHeight")
      _ = if (bestBlockHeight == fromHeight) logger.info(s"Going to index block $bestBlockHeight")
      chainTip <- Future.fromTry(storage.getChainTip)
      chainLinker = new ChainLinker(blockHttpClient.getBlockForId, chainTip)
      syncResult <-
        blockReader.getBlockSource(fromHeight, bench).runWith(blockIndexingSink(chainLinker))
    yield syncResult

}

object StreamExecutor {
  case class ChainSyncResult(
    lastBlock: Option[BestBlockInserted],
    storage: Storage,
    graphTraversalSource: Option[GraphTraversalSource]
  )
}
