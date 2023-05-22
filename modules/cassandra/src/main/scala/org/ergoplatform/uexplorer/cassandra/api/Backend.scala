package org.ergoplatform.uexplorer.cassandra.api

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId, EpochIndex, Height, TxId}
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.cassandra.CassandraBackend
import org.ergoplatform.uexplorer.BlockMetadata
import org.ergoplatform.uexplorer.Tx
import pureconfig.ConfigReader

import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.ConcurrentHashMap
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.{ArraySeq, TreeMap}
import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.Try
import akka.Done
import org.ergoplatform.uexplorer.BoxesByTx
import org.ergoplatform.uexplorer.Epoch
import org.ergoplatform.uexplorer.TopAddressMap
import org.ergoplatform.uexplorer.Epoch.EpochCommand
import org.ergoplatform.uexplorer.Epoch.WriteNewEpoch

trait Backend {

  def removeBlocksFromMainChain(blockIds: List[BlockId]): Future[Done]

  def transactionBoxesByHeightFlow: Flow[Height, (Height, BoxesByTx), NotUsed]

  def blockWriteFlow: Flow[Block, Block, NotUsed]

  def addressWriteFlow: Flow[(Block, Option[EpochCommand]), (Block, Option[EpochCommand]), NotUsed]

  def epochsWriteFlow: Flow[(Block, Option[EpochCommand]), (Block, Option[EpochCommand]), NotUsed]

  def loadBlockInfoByEpochIndex: Future[TreeMap[EpochIndex, BlockMetadata]]

  def close(): Future[Unit]
}

object Backend {
  import pureconfig.generic.derivation.default.*

  sealed trait BackendType derives ConfigReader

  case class CassandraDb(parallelism: Int) extends BackendType

  case object InMemoryDb extends BackendType

  def apply(backendType: BackendType)(implicit system: ActorSystem[Nothing]): Try[Backend] = backendType match {
    case CassandraDb(parallelism) =>
      CassandraBackend(parallelism)
    case InMemoryDb =>
      Try(new InMemoryBackend())
  }

}

class InMemoryBackend extends Backend {

  private val lastBlockInfoByEpochIndex = new ConcurrentHashMap[EpochIndex, BlockMetadata]()

  private val boxesByHeight  = new ConcurrentHashMap[Height, BoxesByTx]()
  private val blocksById     = new ConcurrentHashMap[BlockId, BlockMetadata]()
  private val blocksByHeight = new ConcurrentHashMap[Height, BlockMetadata]()

  def close(): Future[Unit] = Future.successful(())

  override def removeBlocksFromMainChain(blockIds: List[BlockId]): Future[Done] = ???

  def blockWriteFlow: Flow[Block, Block, NotUsed] =
    Flow[Block].map { flatBlock =>
      blocksByHeight.put(flatBlock.header.height, BlockMetadata.fromBlock(flatBlock))
      blocksById.put(flatBlock.header.id, BlockMetadata.fromBlock(flatBlock))
      flatBlock
    }

  def addressWriteFlow: Flow[(Block, Option[EpochCommand]), (Block, Option[EpochCommand]), NotUsed] =
    Flow[(Block, Option[EpochCommand])].map(identity)

  override def epochsWriteFlow: Flow[(Block, Option[EpochCommand]), (Block, Option[EpochCommand]), NotUsed] =
    Flow[(Block, Option[EpochCommand])]
      .map {
        case (block, Some(WriteNewEpoch(epoch, boxesByTxId, topAddresses))) =>
          lastBlockInfoByEpochIndex.put(epoch.index, blocksById.get(epoch.blockIds.last))
          boxesByHeight.putAll(boxesByTxId.asJava)
          block -> Some(WriteNewEpoch(epoch, boxesByTxId, topAddresses))
        case tuple =>
          tuple
      }

  def transactionBoxesByHeightFlow: Flow[Height, (Height, BoxesByTx), NotUsed] =
    Flow[Height].map(height => height -> boxesByHeight.get(height))

  def loadBlockInfoByEpochIndex: Future[TreeMap[EpochIndex, BlockMetadata]] =
    Future.successful(TreeMap(lastBlockInfoByEpochIndex.asScala.toSeq: _*))

}
