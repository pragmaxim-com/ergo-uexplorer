package org.ergoplatform.uexplorer.cassandra.api

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.cassandra.CassandraBackend
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
import org.ergoplatform.uexplorer.Epoch.EpochCommand
import org.ergoplatform.uexplorer.Epoch.WriteNewEpoch
import org.ergoplatform.uexplorer.db.BestBlockInserted

trait Backend {

  def isEmpty: Boolean

  def removeBlocksFromMainChain(blockIds: List[BlockId]): Future[Done]

  def transactionBoxesByHeightFlow: Flow[(Height, BlockId), ((Height, BlockId), BoxesByTx), NotUsed]

  def getAllBlockIdsAndHeight: Source[(Height, BlockId), NotUsed]

  def blockWriteFlow: Flow[BestBlockInserted, BestBlockInserted, NotUsed]

  def addressWriteFlow(addressStats: Address => Option[Address.Stats]): Flow[BestBlockInserted, BestBlockInserted, NotUsed]

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

  private val boxesByHeight     = new ConcurrentHashMap[Height, BoxesByTx]()
  private val blocksById        = new ConcurrentHashMap[BlockId, BlockMetadata]()
  private val blocksByHeight    = new ConcurrentHashMap[Height, BlockMetadata]()
  override def isEmpty: Boolean = true

  override def getAllBlockIdsAndHeight: Source[(Height, BlockId), NotUsed] = ???

  override def close(): Future[Unit] = Future.successful(())

  override def removeBlocksFromMainChain(blockIds: List[BlockId]): Future[Done] = ???

  override def blockWriteFlow: Flow[BestBlockInserted, BestBlockInserted, NotUsed] =
    Flow[BestBlockInserted].map { case blockInserted =>
      blocksByHeight.put(blockInserted.block.header.height, BlockMetadata.fromBlock(blockInserted.block))
      blocksById.put(blockInserted.block.header.id, BlockMetadata.fromBlock(blockInserted.block))
      blockInserted
    }

  override def addressWriteFlow(
    addressStats: Address => Option[Address.Stats]
  ): Flow[BestBlockInserted, BestBlockInserted, NotUsed] =
    Flow[BestBlockInserted].map(identity)

  override def transactionBoxesByHeightFlow: Flow[(Height, BlockId), ((Height, BlockId), BoxesByTx), NotUsed] =
    Flow[(Height, BlockId)].map { case (height, blockId) => (height, blockId) -> boxesByHeight.get(height) }

}
