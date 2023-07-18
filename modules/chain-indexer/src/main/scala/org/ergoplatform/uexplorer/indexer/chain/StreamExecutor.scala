package org.ergoplatform.uexplorer.indexer.chain

import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.ExeContext.Implicits
import org.ergoplatform.uexplorer.chain.{ChainLinker, ChainTip}
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.http.{BlockHttpClient, Codecs}
import org.ergoplatform.uexplorer.indexer.chain.StreamExecutor.*
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import org.ergoplatform.uexplorer.indexer.db.Backend
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.storage.MvStorage
import org.ergoplatform.uexplorer.{BlockId, Height, ProtocolSettings, ReadableStorage}

import java.nio.file.{Path, Paths}
import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent
import scala.collection.immutable.{ListMap, TreeSet}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}
import zio.stream.*
import zio.*

case class StreamExecutor(
  blockHttpClient: BlockHttpClient,
  blockReader: BlockReader,
  blockWriter: BlockWriter,
  storage: ReadableStorage,
  conf: ChainIndexerConf
) extends LazyLogging {

  implicit private val ps: ProtocolSettings = conf.protocol

  def indexNewBlocks: Task[ChainSyncResult] =
    for
      bestBlockHeight <- blockHttpClient.getBestBlockHeight
      fromHeight = storage.getLastHeight.getOrElse(0) + 1
      _ = if (bestBlockHeight > fromHeight) logger.info(s"Going to index blocks from $fromHeight to $bestBlockHeight")
      _ = if (bestBlockHeight == fromHeight) logger.info(s"Going to index block $bestBlockHeight")
      chainTip <- ZIO.fromTry(storage.getChainTip).debug("chainTip")
      chainLinker = new ChainLinker(blockHttpClient.getBlockForId, chainTip)
      syncResult <- blockWriter.insertBranchFlow(blockReader.getBlockSource(fromHeight, conf.benchmarkMode), chainLinker)
    yield syncResult
}

object StreamExecutor {

  def layer: ZLayer[
    BlockHttpClient with BlockReader with BlockWriter with ReadableStorage with ChainIndexerConf,
    Nothing,
    StreamExecutor
  ] =
    ZLayer.fromFunction(StreamExecutor.apply _)

  case class ChainSyncResult(
    lastBlock: Option[BestBlockInserted],
    storage: ReadableStorage,
    graphTraversalSource: GraphTraversalSource
  )
}
