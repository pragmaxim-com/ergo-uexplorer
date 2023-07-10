package org.ergoplatform.uexplorer.cassandra

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.*
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.{bindMarker, insertInto}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.cassandra

import scala.collection.immutable.ArraySeq
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*

trait CassandraPersistenceSupport extends LazyLogging {

  protected[cassandra] def buildInsertStatement(columns: Seq[String], table: String): SimpleStatement = {
    logger.info(s"Building insert statement for table $table with columns ${columns.mkString(", ")}")
    val insertIntoTable = insertInto(cassandra.Const.CassandraKeyspace, table)
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

  protected[cassandra] def storePartitionedBatchFlow[T](
    parallelism: Int,
    maxBatchSize: Int,
    batchType: BatchType = DefaultBatchType.LOGGED,
    simpleStatement: SimpleStatement,
    statementBinder: (T, PreparedStatement) => Source[Seq[BoundStatement], NotUsed]
  )(implicit cqlSession: CqlSession, system: ActorSystem[Nothing]): Flow[T, T, NotUsed] =
    Flow
      .lazyFutureFlow(() =>
        cqlSession.prepareAsync(simpleStatement).asScala.map { preparedStatement =>
          Flow[T]
            .mapAsync(1) { element =>
              statementBinder(element, preparedStatement) match {
                case statements =>
                  statements
                    .mapAsyncUnordered(parallelism) {
                      case batchStatement if batchStatement.isEmpty =>
                        Future.successful(element)
                      case batchStatement if batchStatement.length >= maxBatchSize =>
                        Source
                          .fromIterator(() => batchStatement.grouped(maxBatchSize))
                          .mapAsync(1) { groupedStmnt =>
                            cqlSession
                              .executeAsync(BatchStatement.newInstance(batchType).addAll(groupedStmnt.asJava))
                              .asScala
                          }
                          .run()
                      case batchStatement if batchStatement.length == 1 =>
                        cqlSession
                          .executeAsync(batchStatement.head)
                          .asScala
                      case batchStatement =>
                        cqlSession
                          .executeAsync(BatchStatement.newInstance(batchType).addAll(batchStatement.asJava))
                          .asScala
                    }
                    .run()
                    .map(_ => element)
              }
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
                case statements if statements.length >= 5000 =>
                  Future
                    .sequence(
                      statements.grouped(5000).map { batchStatement =>
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

trait AddressPersistenceSupport {
  protected[cassandra] val node_addresses_table = "node_addresses"

  protected[cassandra] val address               = "address"
  protected[cassandra] val address_partition_idx = "address_partition_idx"
  protected[cassandra] val address_type          = "address_type"
  protected[cassandra] val address_description   = "address_description"
  protected[cassandra] val timestamp             = "timestamp"
  protected[cassandra] val tx_idx                = "tx_idx"
  protected[cassandra] val tx_id                 = "tx_id"
  protected[cassandra] val box_id                = "box_id"
  protected[cassandra] val value                 = "value"

  protected[cassandra] val addressColumns = Seq(
    address,
    address_partition_idx,
    address_type,
    address_description,
    timestamp,
    tx_idx,
    tx_id,
    box_id,
    value
  )

}
