package org.ergoplatform.uexplorer.indexer.chain

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.ReadableStorage
import org.ergoplatform.uexplorer.chain.ChainLinker
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.chain.StreamExecutor.*
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import zio.*
import zio.stream.*

import scala.jdk.CollectionConverters.*

case class StreamExecutor(
  blockHttpClient: BlockHttpClient,
  blockReader: BlockReader,
  blockWriter: BlockWriter,
  storage: ReadableStorage,
  conf: ChainIndexerConf
) {

  def indexNewBlocks: Task[ChainSyncResult] =
    for
      chainTip <- storage.getChainTip
      chainLinker = new ChainLinker(blockHttpClient.getBlockForId, chainTip)(conf.core)
      lastHeight  = storage.getLastHeight.getOrElse(0) + 1
      _ <- ZIO.log(s"Getting blocks from height $lastHeight")
      blockSource = blockReader.getBlockSource(lastHeight, conf.benchmarkMode)
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
