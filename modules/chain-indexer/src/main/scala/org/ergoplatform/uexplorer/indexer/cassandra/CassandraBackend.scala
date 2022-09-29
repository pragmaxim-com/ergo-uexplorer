package org.ergoplatform.uexplorer.indexer.cassandra

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
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
  with CassandraBlockInfoWriter
  with CassandraTransactionsWriter
  with CassandraAssetsWriter
  with CassandraRegistersWriter
  with CassandraTokensWriter
  with CassandraInputsWriter
  with CassandraOutputsWriter
  with CassandraBlockUpdater
  with CassandraEpochWriter
  with CassandraEpochReader {

  protected[cassandra] def buildInsertStatement(columns: Seq[String], table: String): SimpleStatement = {
    import QueryBuilder.{bindMarker, insertInto}
    logger.info(s"Building insert statement for $table")
    val insertIntoTable = insertInto(Const.CassandraKeyspace, table)
    columns.tail
      .foldLeft(insertIntoTable.value(columns.head, bindMarker(columns.head))) { case (acc, column) =>
        acc.value(column, bindMarker(column))
      }
      .build()
      .setIdempotent(true)
  }

  val blockWriteFlow: Flow[Inserted, FlatBlock, NotUsed] =
    Flow[Inserted]
      // format: off
      .via(blockUpdaterFlow(parallelism))
      .via(headerWriteFlow(parallelism)).buffer(Const.BufferSize, OverflowStrategy.backpressure)
      .via(blockInfoWriteFlow(parallelism)).buffer(Const.BufferSize, OverflowStrategy.backpressure)
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
