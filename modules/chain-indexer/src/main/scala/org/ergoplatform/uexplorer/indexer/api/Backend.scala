package org.ergoplatform.uexplorer.indexer.api

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.Flow
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId, TxId}
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
import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.Try

trait Backend {

  def graphTraversalSource: GraphTraversalSource

  def blockWriteFlow: Flow[Inserted, Block, NotUsed]

  def epochsWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed]

  def graphWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed]

  def loadUtxoState(epochIndexes: Iterator[Int]): Future[UtxoState]

  def loadBlockInfoByEpochIndex: Future[TreeMap[Int, BufferedBlockInfo]]

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

  private val lastBlockInfoByEpochIndex = new ConcurrentHashMap[Int, BufferedBlockInfo]()

  private val boxesByHeight =
    new ConcurrentHashMap[Int, ArraySeq[(Tx, (ArraySeq[(BoxId, Address, Long)], ArraySeq[(BoxId, Address, Long)]))]]()
  private val blocksById     = new ConcurrentHashMap[BlockId, BufferedBlockInfo]()
  private val blocksByHeight = new ConcurrentHashMap[Int, BufferedBlockInfo]()

  def close(): Future[Unit] = Future.successful(())

  def graphTraversalSource: GraphTraversalSource = EmptyGraph.instance.traversal()

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

  override def epochsWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed] =
    Flow[(Block, Option[MaybeNewEpoch])]
      .map {
        case (block, Some(NewEpochDetected(epoch, boxesByTxId))) =>
          lastBlockInfoByEpochIndex.put(epoch.index, blocksById.get(epoch.blockIds.last))
          block -> Some(NewEpochDetected(epoch, boxesByTxId))
        case tuple =>
          tuple
      }

  override def graphWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed] =
    Flow[(Block, Option[MaybeNewEpoch])]
      .map {
        case (block, Some(NewEpochDetected(epoch, boxesByTxIdByHeight))) =>
          boxesByHeight.putAll(boxesByTxIdByHeight.asJava)
          block -> Some(NewEpochDetected(epoch, boxesByTxIdByHeight))
        case tuple =>
          tuple
      }

  override def loadUtxoState(epochIndexes: Iterator[Int]): Future[UtxoState] =
    Future(
      UtxoState.empty.mergeBufferedBoxes(Some(epochIndexes.flatMap(Epoch.heightRangeForEpochIndex).toSeq))._2
    )

  def loadBlockInfoByEpochIndex: Future[TreeMap[Int, BufferedBlockInfo]] =
    Future.successful(TreeMap(lastBlockInfoByEpochIndex.asScala.toSeq: _*))

}
