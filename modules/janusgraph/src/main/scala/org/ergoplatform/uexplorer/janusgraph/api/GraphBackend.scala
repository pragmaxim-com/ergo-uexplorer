package org.ergoplatform.uexplorer.janusgraph.api

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.{Graph, Transaction}
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.db.{BestBlockInserted, BlockInfo, BlockWithInputs, FullBlock}
import pureconfig.ConfigReader
import org.ergoplatform.uexplorer.janusgraph.JanusGraphBackend

import java.util.concurrent.ConcurrentHashMap
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.{ArraySeq, TreeMap}
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.{Success, Try}

trait GraphBackend {

  def initGraph: Boolean

  def graphWriteFlow: Flow[BestBlockInserted, BestBlockInserted, NotUsed]

  def writeTxsAndCommit(blocks: Seq[BestBlockInserted]): IterableOnce[BestBlockInserted]

  def graphTraversalSource: GraphTraversalSource

  def tx: Transaction

  def isEmpty: Boolean

  def close(): Future[Unit]
}

object GraphBackend {
  import pureconfig.generic.derivation.default.*
  sealed trait GraphBackendType derives ConfigReader
  case object JanusGraph extends GraphBackendType
  case object InMemoryGraph extends GraphBackendType
  case object NoGraphBackend extends GraphBackendType

  def apply(graphBackendType: GraphBackendType)(implicit system: ActorSystem[Nothing]): Try[Option[GraphBackend]] =
    graphBackendType match {
      case JanusGraph =>
        JanusGraphBackend().map(Some(_))
      case InMemoryGraph =>
        Try(Some(new InMemoryGraphBackend()))
      case NoGraphBackend =>
        Success(None)
    }

}

class InMemoryGraphBackend extends GraphBackend {

  private var initialized = true

  def initGraph: Boolean =
    if (initialized) {
      initialized = false
      true
    } else {
      false
    }

  def tx: Transaction = ???

  def writeTxsAndCommit(blocks: Seq[BestBlockInserted]): IterableOnce[BestBlockInserted] = List.empty

  def isEmpty: Boolean = true

  def graphTraversalSource: GraphTraversalSource = EmptyGraph.instance.traversal()

  def graphWriteFlow: Flow[BestBlockInserted, BestBlockInserted, NotUsed] =
    Flow[BestBlockInserted].map(identity)

  def close(): Future[Unit] = Future.successful(())
}
