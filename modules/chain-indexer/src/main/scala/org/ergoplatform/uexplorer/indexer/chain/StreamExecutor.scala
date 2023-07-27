package org.ergoplatform.uexplorer.indexer.chain

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
import org.ergoplatform.uexplorer.{BlockId, CoreConf, Height, ReadableStorage}

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
) {

  def indexNewBlocks: Task[ChainSyncResult] =
    for
      _        <- blockHttpClient.getBestBlockHeight
      chainTip <- storage.getChainTip
      chainLinker = new ChainLinker(blockHttpClient.getBlockForId, chainTip)(conf.core)
      blockSource = blockReader.getBlockSource(storage.getLastHeight.getOrElse(0) + 1, conf.benchmarkMode)
      syncResult <- blockWriter.insertBranchFlow(blockSource, chainLinker)
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
