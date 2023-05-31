package org.ergoplatform.uexplorer.janusgraph.api

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.{Graph, Transaction}
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.BlockMetadata
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.Epoch.EpochCommand
import pureconfig.ConfigReader
import org.ergoplatform.uexplorer.janusgraph.JanusGraphBackend

import java.util.concurrent.ConcurrentHashMap
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.{ArraySeq, TreeMap}
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.Try
import org.ergoplatform.uexplorer.db.BestBlockInserted

trait GraphBackend {

  def initGraph: Boolean

  def graphWriteFlow(addressStats: Address => Option[Address.Stats]): Flow[BestBlockInserted, BestBlockInserted, NotUsed]

  def writeTx(height: Height, boxesByTx: BoxesByTx, addressStats: Address => Option[Address.Stats], g: Graph): Unit

  def writeTxsAndCommit(
    txBoxesByHeight: IterableOnce[BestBlockInserted],
    addressStats: Address => Option[Address.Stats]
  ): Unit

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

  def apply(graphBackendType: GraphBackendType)(implicit system: ActorSystem[Nothing]): Try[GraphBackend] =
    graphBackendType match {
      case JanusGraph =>
        JanusGraphBackend()
      case InMemoryGraph =>
        Try(new InMemoryGraphBackend())
    }

}

class InMemoryGraphBackend extends GraphBackend {

  def initGraph: Boolean = false

  def tx: Transaction = ???

  def writeTx(height: Height, boxesByTx: BoxesByTx, addressStats: Address => Option[Address.Stats], g: Graph): Unit = {}

  def writeTxsAndCommit(
    txBoxesByHeight: IterableOnce[BestBlockInserted],
    addressStats: Address => Option[Address.Stats]
  ): Unit = {}
  def isEmpty: Boolean = true

  def graphTraversalSource: GraphTraversalSource = EmptyGraph.instance.traversal()

  def graphWriteFlow(addressStats: Address => Option[Address.Stats]): Flow[BestBlockInserted, BestBlockInserted, NotUsed] =
    Flow[BestBlockInserted].map(identity)

  def close(): Future[Unit] = Future.successful(())
}
