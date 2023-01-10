package org.ergoplatform.uexplorer.indexer.cassandra

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.*

import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.{bindMarker, insertInto}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.{indexer, Const}

import scala.collection.immutable.ArraySeq

trait CassandraPersistenceSupport extends LazyLogging {

  protected[cassandra] def buildInsertStatement(columns: Seq[String], table: String): SimpleStatement = {
    logger.info(s"Building insert statement for table $table with columns ${columns.mkString(", ")}")
    val insertIntoTable = insertInto(indexer.Const.CassandraKeyspace, table)
    columns.tail
      .foldLeft(insertIntoTable.value(columns.head, bindMarker(columns.head))) { case (acc, column) =>
        acc.value(column, bindMarker(column))
      }
      .build()
      .setIdempotent(true)
  }

  protected[cassandra] def storeFlow[T](
    parallelism: Int,
    simpleStatement: SimpleStatement,
    statementBinder: (T, PreparedStatement) => BoundStatement
  )(implicit cqlSession: CqlSession): Flow[T, T, NotUsed] =
    Flow
      .lazyFutureFlow(() =>
        cqlSession.prepareAsync(simpleStatement).asScala.map { preparedStatement =>
          Flow[T]
            .mapAsync(parallelism) { element =>
              cqlSession
                .executeAsync(statementBinder(element, preparedStatement))
                .asScala
                .map(_ => element)
            }
        }
      )
      .mapMaterializedValue(_ => NotUsed)

  protected[cassandra] def storeBatchFlow[T](
    parallelism: Int,
    batchType: BatchType = DefaultBatchType.LOGGED,
    simpleStatement: SimpleStatement,
    statementBinder: (T, PreparedStatement) => ArraySeq[BoundStatement]
  )(implicit cqlSession: CqlSession): Flow[T, T, NotUsed] =
    Flow
      .lazyFutureFlow(() =>
        cqlSession.prepareAsync(simpleStatement).asScala.map { preparedStatement =>
          Flow[T]
            .mapAsync(parallelism) { element =>
              statementBinder(element, preparedStatement) match {
                case statements if statements.isEmpty =>
                  Future.successful(element)
                case statements if statements.length >= 10000 =>
                  Future
                    .sequence(
                      statements.grouped(10000).map { batchStatement =>
                        cqlSession
                          .executeAsync(BatchStatement.newInstance(batchType).addAll(batchStatement.asJava))
                          .asScala
                      }
                    )
                    .map(_ => element)
                case statements if statements.length == 1 =>
                  cqlSession
                    .executeAsync(statements.head)
                    .asScala
                    .map(_ => element)
                case statements =>
                  cqlSession
                    .executeAsync(BatchStatement.newInstance(batchType).addAll(statements.asJava))
                    .asScala
                    .map(_ => element)
              }
            }
        }
      )
      .mapMaterializedValue(_ => NotUsed)
}

trait EpochPersistenceSupport {
  protected[cassandra] val node_epoch_last_headers_table = "node_epoch_last_headers"
  protected[cassandra] val node_epochs_inputs_table      = "node_epoch_inputs"
  protected[cassandra] val node_epochs_outputs_table     = "node_epoch_outputs"

  protected[cassandra] val epoch_index    = "epoch_index"
  protected[cassandra] val last_header_id = "last_header_id"
  protected[cassandra] val tx_id          = "tx_id"
  protected[cassandra] val tx_idx         = "tx_idx"
  protected[cassandra] val box_id         = "box_id"
  protected[cassandra] val address        = "address"
  protected[cassandra] val value          = "value"

}
