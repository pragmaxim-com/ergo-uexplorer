package org.ergoplatform.uexplorer.cassandra.api

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.cassandra.CassandraBackend
import pureconfig.ConfigReader

import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.ConcurrentHashMap
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.{ArraySeq, TreeMap}
import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.{Success, Try}
import akka.Done
import org.ergoplatform.uexplorer.db.{BestBlockInserted, BlockInfo, FullBlock}

trait Backend {

  def isEmpty: Boolean

  def removeBlocksFromMainChain(blockIds: Iterable[BlockId]): Future[Done]

  def blockWriteFlow: Flow[BestBlockInserted, BestBlockInserted, NotUsed]

  def close(): Future[Unit]
}

object Backend {
  import pureconfig.generic.derivation.default.*

  sealed trait BackendType derives ConfigReader

  case class CassandraDb(parallelism: Int) extends BackendType

  case object InMemoryDb extends BackendType

  case object NoBackend extends BackendType

  def apply(backendType: BackendType)(implicit system: ActorSystem[Nothing]): Try[Option[Backend]] = backendType match {
    case CassandraDb(parallelism) =>
      CassandraBackend(parallelism).map(Some(_))
    case InMemoryDb =>
      Try(Some(new InMemoryBackend()))
    case NoBackend =>
      Success(None)
  }

}

class InMemoryBackend extends Backend {

  private val blocksById        = new ConcurrentHashMap[BlockId, BlockInfo]()
  private val blocksByHeight    = new ConcurrentHashMap[Height, BlockInfo]()
  override def isEmpty: Boolean = true

  override def close(): Future[Unit] = Future.successful(())

  override def removeBlocksFromMainChain(blockIds: Iterable[BlockId]): Future[Done] =
    Future(blockIds.foreach(blocksById.remove)).map(_ => Done)

  override def blockWriteFlow: Flow[BestBlockInserted, BestBlockInserted, NotUsed] =
    Flow[BestBlockInserted].map { blockInserted =>
      blocksByHeight.put(blockInserted.lightBlock.info.height, blockInserted.lightBlock.info)
      blocksById.put(blockInserted.lightBlock.b.header.id, blockInserted.lightBlock.info)
      blockInserted
    }

}
