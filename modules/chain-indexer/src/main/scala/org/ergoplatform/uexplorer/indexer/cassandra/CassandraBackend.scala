package org.ergoplatform.uexplorer.indexer.cassandra

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.uexplorer.indexer.Const
import org.ergoplatform.uexplorer.indexer.api.Backend
import org.ergoplatform.uexplorer.indexer.cassandra.entity._
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor.Inserted

import scala.concurrent.duration.DurationInt

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
  with CassandraEpochReader {

  val blockWriteFlow: Flow[Inserted, FlatBlock, NotUsed] =
    Flow[Inserted]
      // format: off
      .via(blockUpdaterFlow(parallelism))
      .via(headerWriteFlow(parallelism)).buffer(Const.BufferSize, OverflowStrategy.backpressure)
      .via(transactionsWriteFlow(parallelism)).buffer(Const.BufferSize, OverflowStrategy.backpressure)
      .via(registersWriteFlow(parallelism)).buffer(Const.BufferSize, OverflowStrategy.backpressure)
      .via(tokensWriteFlow(parallelism)).buffer(Const.BufferSize, OverflowStrategy.backpressure)
      .via(inputsWriteFlow(parallelism)).buffer(Const.BufferSize, OverflowStrategy.backpressure)
      .via(assetsWriteFlow(parallelism)).buffer(Const.BufferSize, OverflowStrategy.backpressure)
      .via(outputsWriteFlow(parallelism))
      // format: on
}

object CassandraBackend {

  import akka.stream.alpakka.cassandra.CassandraSessionSettings
  import akka.stream.alpakka.cassandra.scaladsl.{CassandraSession, CassandraSessionRegistry}
  import com.datastax.oss.driver.api.core.CqlSession

  import scala.concurrent.Await

  def apply(parallelism: Int)(implicit system: ActorSystem[Nothing]): CassandraBackend = {
    val cassandraSession: CassandraSession =
      CassandraSessionRegistry.get(system).sessionFor(CassandraSessionSettings())
    implicit val cqlSession: CqlSession = Await.result(cassandraSession.underlying(), 5.seconds)
    new CassandraBackend(parallelism)
  }
}
