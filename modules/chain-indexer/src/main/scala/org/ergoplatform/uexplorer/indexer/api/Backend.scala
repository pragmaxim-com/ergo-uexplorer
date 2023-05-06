package org.ergoplatform.uexplorer.indexer.api

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId, EpochIndex, Height, TxId}
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.cassandra.CassandraBackend
import org.ergoplatform.uexplorer.indexer.chain.{ChainState, Epoch}
import org.ergoplatform.uexplorer.indexer.chain.ChainState.BufferedBlockInfo
import org.ergoplatform.uexplorer.indexer.chain.ChainStateHolder.*
import org.ergoplatform.uexplorer.indexer.config.{BackendType, CassandraDb, InMemoryDb}
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState.Tx

import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.ConcurrentHashMap
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.{ArraySeq, TreeMap}
import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.Try

trait Backend {

  def transactionBoxesByHeightFlow: Flow[Height, (Height, UtxoState.BoxesByTx), NotUsed]

  def blockWriteFlow: Flow[Inserted, Block, NotUsed]

  def addressWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed]

  def epochsWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed]

  def loadBlockInfoByEpochIndex: Future[TreeMap[EpochIndex, BufferedBlockInfo]]

  def close(): Future[Unit]
}

object Backend {

  def apply(backendType: BackendType)(implicit system: ActorSystem[Nothing]): Try[Backend] = backendType match {
    case CassandraDb(parallelism) =>
      CassandraBackend(parallelism)
    case InMemoryDb =>
      Try(new InMemoryBackend())
  }

}

class InMemoryBackend extends Backend {

  private val lastBlockInfoByEpochIndex = new ConcurrentHashMap[EpochIndex, BufferedBlockInfo]()

  private val boxesByHeight  = new ConcurrentHashMap[Height, UtxoState.BoxesByTx]()
  private val blocksById     = new ConcurrentHashMap[BlockId, BufferedBlockInfo]()
  private val blocksByHeight = new ConcurrentHashMap[Height, BufferedBlockInfo]()

  def close(): Future[Unit] = Future.successful(())

  override def blockWriteFlow: Flow[Inserted, Block, NotUsed] =
    Flow[Inserted]
      .mapConcat {
        case BestBlockInserted(flatBlock) =>
          blocksByHeight.put(flatBlock.header.height, BufferedBlockInfo.fromBlock(flatBlock))
          blocksById.put(flatBlock.header.id, BufferedBlockInfo.fromBlock(flatBlock))
          List(flatBlock)
        case ForkInserted(winningFork, _) =>
          winningFork.foreach { flatBlock =>
            blocksByHeight.put(flatBlock.header.height, BufferedBlockInfo.fromBlock(flatBlock))
            blocksById.put(flatBlock.header.id, BufferedBlockInfo.fromBlock(flatBlock))
          }
          winningFork
      }

  def addressWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed] =
    Flow[(Block, Option[MaybeNewEpoch])].map(identity)

  override def epochsWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed] =
    Flow[(Block, Option[MaybeNewEpoch])]
      .map {
        case (block, Some(NewEpochDetected(epoch, boxesByTxId, topAddresses))) =>
          lastBlockInfoByEpochIndex.put(epoch.index, blocksById.get(epoch.blockIds.last))
          boxesByHeight.putAll(boxesByTxId.asJava)
          block -> Some(NewEpochDetected(epoch, boxesByTxId, topAddresses))
        case tuple =>
          tuple
      }

  def transactionBoxesByHeightFlow: Flow[Height, (Height, UtxoState.BoxesByTx), NotUsed] =
    Flow[Height].map(height => height -> boxesByHeight.get(height))

  def loadBlockInfoByEpochIndex: Future[TreeMap[EpochIndex, BufferedBlockInfo]] =
    Future.successful(TreeMap(lastBlockInfoByEpochIndex.asScala.toSeq: _*))

}
