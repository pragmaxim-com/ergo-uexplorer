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

import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.FutureConverters.*
import java.net.InetSocketAddress
import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.jdk.CollectionConverters.*
import scala.concurrent.duration.DurationInt
import CassandraBackend.BufferSize
import akka.actor.CoordinatedShutdown

import scala.concurrent.Future

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
  with CassandraEpochReader
  with CassandraUtxoReader {

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

  def apply(parallelism: Int)(implicit system: ActorSystem[Nothing]): CassandraBackend = {
    implicit val cqlSession: CqlSession =
      CqlSession
        .builder()
        .withConfigLoader(new CassandraConfigLoader(system.settings.config.getConfig("datastax-java-driver")))
        .build()
    logger.info(s"Cassandra session created")
    val backend = new CassandraBackend(parallelism)
    CoordinatedShutdown(system).addTask(
      CoordinatedShutdown.PhaseBeforeServiceUnbind,
      "stop-cassandra-backend"
    ) { () =>
      backend.close().map(_ => Done)
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
