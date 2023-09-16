package org.ergoplatform.uexplorer.indexer.chain

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.ReadableStorage
import org.ergoplatform.uexplorer.chain.{ChainLinker, ChainTip}
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.chain.StreamExecutor.*
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import zio.*
import zio.stream.*

import scala.jdk.CollectionConverters.*

case class StreamExecutor(
  blockReader: BlockReader,
  blockWriter: BlockWriter
) {

  def indexNewBlocks(chainLinker: ChainLinker, lastHeight: Int, benchmarkMode: Boolean): Task[ChainSyncResult] =
    for
      _ <- ZIO.log(s"Getting blocks from height $lastHeight")
      blockSource = blockReader.getBlockSource(lastHeight, benchmarkMode)
      syncResult <- blockWriter.insertBranchFlow(blockSource, chainLinker)
    yield syncResult
}

object StreamExecutor {

  def layer: ZLayer[
    BlockReader with BlockWriter,
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
