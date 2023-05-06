package org.ergoplatform.uexplorer.indexer.api

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.cassandra.CassandraBackend
import org.ergoplatform.uexplorer.indexer.chain.ChainState.BufferedBlockInfo
import org.ergoplatform.uexplorer.indexer.chain.ChainStateHolder.*
import org.ergoplatform.uexplorer.indexer.chain.{ChainState, Epoch}
import org.ergoplatform.uexplorer.indexer.config.*
import org.ergoplatform.uexplorer.indexer.janusgraph.JanusGraphBackend
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState.{BoxesByTx, Tx}
import org.ergoplatform.uexplorer.*

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

  def graphWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed]

  def writeTxsAndCommit(txBoxesByHeight: IterableOnce[(Height, BoxesByTx)], topAddresses: TopAddressMap): Unit

  def graphTraversalSource: GraphTraversalSource

  def isEmpty: Boolean

  def close(): Future[Unit]
}

object GraphBackend {

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

  def graphWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed] =
    Flow[(Block, Option[MaybeNewEpoch])].map(identity)

  def writeTxsAndCommit(txBoxesByHeight: IterableOnce[(Height, BoxesByTx)], topAddresses: TopAddressMap): Unit = ()

  def close(): Future[Unit] = Future.successful(())
}
