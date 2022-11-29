package org.ergoplatform.uexplorer.indexer.cassandra

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.*

import scala.jdk.CollectionConverters.*
import scala.compat.java8.FutureConverters.*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.{bindMarker, insertInto}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.Const

import scala.collection.immutable.ArraySeq

trait CassandraPersistenceSupport extends LazyLogging {

  protected[cassandra] def buildInsertStatement(columns: Seq[String], table: String): SimpleStatement = {
    logger.info(s"Building insert statement for table $table with columns ${columns.mkString(", ")}")
    val insertIntoTable = insertInto(Const.CassandraKeyspace, table)
    columns.tail
      .foldLeft(insertIntoTable.value(columns.head, bindMarker(columns.head))) { case (acc, column) =>
        acc.value(column, bindMarker(column))
      }
      .build()
      .setIdempotent(true)
  }

  protected[cassandra] def storeBlockFlow[T](
    parallelism: Int,
    simpleStatement: SimpleStatement,
    statementBinder: (T, PreparedStatement) => BoundStatement
  )(implicit cqlSession: CqlSession): Flow[T, T, NotUsed] =
    Flow
      .lazyFutureFlow(() =>
        cqlSession.prepareAsync(simpleStatement).toScala.map { preparedStatement =>
          Flow[T]
            .mapAsync(parallelism) { element =>
              cqlSession
                .executeAsync(statementBinder(element, preparedStatement))
                .toScala
                .map(_ => element)
            }
        }
      )
      .mapMaterializedValue(_ => NotUsed)

  protected[cassandra] def storeBlockBatchFlow[T](
    parallelism: Int,
    batchType: BatchType = DefaultBatchType.LOGGED,
    simpleStatement: SimpleStatement,
    statementBinder: (T, PreparedStatement) => ArraySeq[BoundStatement]
  )(implicit cqlSession: CqlSession): Flow[T, T, NotUsed] =
    Flow
      .lazyFutureFlow(() =>
        cqlSession.prepareAsync(simpleStatement).toScala.map { preparedStatement =>
          Flow[T]
            .mapAsync(parallelism) { element =>
              statementBinder(element, preparedStatement) match {
                case statements if statements.isEmpty =>
                  Future.successful(element)
                case statements if statements.length == 1 =>
                  cqlSession
                    .executeAsync(statements.head)
                    .toScala
                    .map(_ => element)
                case statements =>
                  cqlSession
                    .executeAsync(BatchStatement.newInstance(batchType).addAll(statements.asJava))
                    .toScala
                    .map(_ => element)
              }
            }
        }
      )
      .mapMaterializedValue(_ => NotUsed)
}

trait EpochPersistenceSupport {
  protected[cassandra] val node_epochs_table = "node_epochs"

  protected[cassandra] val epoch_index      = "epoch_index"
  protected[cassandra] val last_header_id   = "last_header_id"
  protected[cassandra] val input_box_ids    = "input_box_ids"
  protected[cassandra] val utxos_by_address = "utxos_by_address"

}
