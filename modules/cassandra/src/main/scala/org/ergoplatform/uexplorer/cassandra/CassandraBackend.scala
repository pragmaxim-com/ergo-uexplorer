package org.ergoplatform.uexplorer.cassandra

import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
import akka.{Done, NotUsed}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.{DriverConfig, DriverConfigLoader}
import com.datastax.oss.driver.api.core.context.DriverContext
import com.datastax.oss.driver.internal.core.config.typesafe.TypesafeDriverConfig
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.{Direction, T}
import org.ergoplatform.uexplorer.Const
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.cassandra.CassandraBackend.BufferSize
import org.ergoplatform.uexplorer.cassandra.entity.*

import java.net.InetSocketAddress
import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*
import scala.util.Try
import org.ergoplatform.uexplorer.cassandra.api.Backend

class CassandraBackend(parallelism: Int)(implicit
  val cqlSession: CqlSession,
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
  with CassandraAddressWriter
  with CassandraEpochReader
  with CassandraUtxoReader {

  def close(): Future[Unit] = {
    logger.info(s"Stopping Cassandra session")
    cqlSession.closeAsync().toCompletableFuture.asScala.map(_ => ())
  }

  val blockWriteFlow: Flow[Block, Block, NotUsed] =
    Flow[Block]
      // format: off
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
    logger.info(s"Cassandra session created")
    val backend = new CassandraBackend(parallelism)
    CoordinatedShutdown(system).addTask(
      CoordinatedShutdown.PhaseServiceStop,
      "stop-cassandra-backend"
    ) { () =>
      backend.close().map { _ =>
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
