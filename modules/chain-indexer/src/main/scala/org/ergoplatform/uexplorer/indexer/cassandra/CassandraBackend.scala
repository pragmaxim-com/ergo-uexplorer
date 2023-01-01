package org.ergoplatform.uexplorer.indexer.cassandra

import akka.{Done, NotUsed}
import akka.actor.typed.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.{DriverConfig, DriverConfigLoader}
import com.datastax.oss.driver.api.core.context.DriverContext
import com.datastax.oss.driver.internal.core.config.typesafe.TypesafeDriverConfig
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.Const
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.api.Backend
import org.ergoplatform.uexplorer.indexer.cassandra.entity.*
import org.ergoplatform.uexplorer.indexer.chain.ChainStateHolder.Inserted
import org.apache.tinkerpop.gremlin.structure.T

import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.FutureConverters.*
import java.net.InetSocketAddress
import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.jdk.CollectionConverters.*
import scala.concurrent.duration.DurationInt
import CassandraBackend.BufferSize
import akka.actor.CoordinatedShutdown
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.indexer.Utils
import org.janusgraph.core.{JanusGraph, JanusGraphFactory, Multiplicity}
import org.janusgraph.graphdb.database.StandardJanusGraph
import org.janusgraph.graphdb.types.vertices.PropertyKeyVertex

import scala.concurrent.Future
import scala.util.Try

class CassandraBackend(parallelism: Int)(implicit
  val cqlSession: CqlSession,
  val janusGraph: StandardJanusGraph,
  val system: ActorSystem[Nothing]
) extends Backend
  with LazyLogging
  with CassandraPersistenceSupport
  with CassandraHeaderWriter
  with CassandraTransactionsWriter
  with CassandraAssetsWriter
  with CassandraRegistersWriter
  with CassandraTokensWriter
  with CassandraInputsWriter
  with CassandraOutputsWriter
  with CassandraBlockUpdater
  with CassandraEpochWriter
  with CassandraEpochReader
  with CassandraUtxoReader {

  def graphTraversalSource: GraphTraversalSource = janusGraph.traversal()

  def close(): Future[Unit] = {
    logger.info(s"Stopping Cassandra session")
    cqlSession.closeAsync().toCompletableFuture.asScala.map(_ => ())
  }

  val blockWriteFlow: Flow[Inserted, Block, NotUsed] =
    Flow[Inserted]
      // format: off
      .via(blockUpdaterFlow(parallelism))
      .via(headerWriteFlow(parallelism)).buffer(BufferSize, OverflowStrategy.backpressure)
      .via(transactionsWriteFlow(parallelism)).buffer(BufferSize, OverflowStrategy.backpressure)
      .via(registersWriteFlow(parallelism)).buffer(BufferSize, OverflowStrategy.backpressure)
      .via(tokensWriteFlow(parallelism)).buffer(BufferSize, OverflowStrategy.backpressure)
      .via(inputsWriteFlow(parallelism)).buffer(BufferSize, OverflowStrategy.backpressure)
      .via(assetsWriteFlow(parallelism)).buffer(BufferSize, OverflowStrategy.backpressure)
      .via(outputsWriteFlow(parallelism))
      // format: on
}

object CassandraBackend extends LazyLogging {

  import com.datastax.oss.driver.api.core.CqlSession

  import scala.concurrent.Await
  val BufferSize = 16

  def apply(parallelism: Int)(implicit system: ActorSystem[Nothing]): Try[CassandraBackend] = Try {
    val datastaxDriverConf = system.settings.config.getConfig("datastax-java-driver")
    implicit val cqlSession: CqlSession =
      CqlSession
        .builder()
        .withConfigLoader(new CassandraConfigLoader(datastaxDriverConf))
        .build()
    implicit val janusGraph = JanusGraphFactory.build
      .set("storage.backend", "cql")
      .set("storage.hostname", datastaxDriverConf.getStringList("basic.contact-points").get(0))
      .set("storage.transactions", false)
      .set("storage.batch-loading", true)
      .set("graph.set-vertex-id", true)
      .open()
      .asInstanceOf[StandardJanusGraph]

    val mgmt = janusGraph.openManagement()
    if (!mgmt.containsGraphIndex("byAddress") || !mgmt.containsEdgeLabel("spentBy")) {
      logger.info("Creating Janus index 'byAddress' and edge label 'spentBy'")
      val address = mgmt.getOrCreatePropertyKey("address")
      mgmt.buildIndex("byAddress", classOf[PropertyKeyVertex]).addKey(address).buildCompositeIndex()
      mgmt.makeEdgeLabel("spentBy").multiplicity(Multiplicity.SIMPLE).make()
      mgmt.commit()

      val vertexCountBound = janusGraph.getIDManager.getVertexCountBound
      logger.info(s"Max vertex count bound: $vertexCountBound")
      logger.info("Creating vertices for genesis boxes")
      val tx = janusGraph.newTransaction()
      Const.genesisBoxes.foreach { boxId =>
        tx.addVertex(T.id, Utils.vertexHash(boxId))
      }
      tx.commit()
    }

    logger.info(s"Cassandra session and Janus graph created")
    val backend = new CassandraBackend(parallelism)
    CoordinatedShutdown(system).addTask(
      CoordinatedShutdown.PhaseServiceUnbind,
      "stop-cassandra-backend"
    ) { () =>
      backend.close().map { _ =>
        janusGraph.close()
        Done
      }
    }
    backend
  }

  class CassandraConfigLoader(config: Config) extends DriverConfigLoader {

    private val driverConfig: DriverConfig = new TypesafeDriverConfig(config)

    override def getInitialConfig: DriverConfig = driverConfig

    override def onDriverInit(context: DriverContext): Unit = ()

    override def reload(): CompletionStage[java.lang.Boolean] = CompletableFuture.completedFuture(false)

    override def supportsReloading(): Boolean = false

    override def close(): Unit = ()
  }

}
