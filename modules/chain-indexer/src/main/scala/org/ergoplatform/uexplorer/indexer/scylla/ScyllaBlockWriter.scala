package org.ergoplatform.uexplorer.indexer.scylla

import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.uexplorer.indexer.scylla.entity._
import org.ergoplatform.uexplorer.indexer.Const
import org.ergoplatform.uexplorer.indexer.api.BlockWriter

class ScyllaBlockWriter(implicit
  val cqlSession: CqlSession
) extends BlockWriter
  with LazyLogging
  with ScyllaPersistenceSupport
  with ScyllaHeaderWriter
  with ScyllaBlockInfoWriter
  with ScyllaTransactionsWriter
  with ScyllaAssetsWriter
  with ScyllaRegistersWriter
  with ScyllaTokensWriter
  with ScyllaInputsWriter
  with ScyllaOutputsWriter {

  protected[scylla] def buildInsertStatement(columns: Seq[String], table: String): SimpleStatement = {
    import QueryBuilder.{bindMarker, insertInto}
    logger.info(s"Building insert statement for $table")
    val insertIntoTable = insertInto(Const.ScyllaKeyspace, table)
    columns.tail
      .foldLeft(insertIntoTable.value(columns.head, bindMarker(columns.head))) { case (acc, column) =>
        acc.value(column, bindMarker(column))
      }
      .build()
      .setIdempotent(true)
  }

  val blockWriteFlow: Flow[FlatBlock, FlatBlock, NotUsed] =
    Flow[FlatBlock]
      // format: off
      .via(headerWriteFlow(parallelism = 1)).buffer(32, OverflowStrategy.backpressure)
      .via(blockInfoWriteFlow(parallelism = 1)).buffer(32, OverflowStrategy.backpressure)
      .via(transactionsWriteFlow(parallelism = 1)).buffer(32, OverflowStrategy.backpressure)
      .via(registersWriteFlow(parallelism = 1)).buffer(32, OverflowStrategy.backpressure)
      .via(tokensWriteFlow(parallelism = 1)).buffer(32, OverflowStrategy.backpressure)
      .via(inputsWriteFlow(parallelism = 1)).buffer(32, OverflowStrategy.backpressure)
      .via(assetsWriteFlow(parallelism = 1)).buffer(32, OverflowStrategy.backpressure)
      .via(outputsWriteFlow(parallelism = 1)).buffer(32, OverflowStrategy.backpressure)
      // format: on
}
