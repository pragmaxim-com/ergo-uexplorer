package org.ergoplatform.uexplorer.janusgraph.api

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
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

trait GraphBackend {

  def initGraph: Boolean

  def graphWriteFlow: Flow[(Block, Option[EpochCommand]), (Block, Option[EpochCommand]), NotUsed]

  def writeTxsAndCommit(txBoxesByHeight: IterableOnce[(Height, BoxesByTx)], topAddresses: TopAddressMap): Unit

  def graphTraversalSource: GraphTraversalSource

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

  def isEmpty: Boolean = true

  def graphTraversalSource: GraphTraversalSource = EmptyGraph.instance.traversal()

  def graphWriteFlow: Flow[(Block, Option[EpochCommand]), (Block, Option[EpochCommand]), NotUsed] =
    Flow[(Block, Option[EpochCommand])].map(identity)

  def writeTxsAndCommit(txBoxesByHeight: IterableOnce[(Height, BoxesByTx)], topAddresses: TopAddressMap): Unit = ()

  def close(): Future[Unit] = Future.successful(())
}
