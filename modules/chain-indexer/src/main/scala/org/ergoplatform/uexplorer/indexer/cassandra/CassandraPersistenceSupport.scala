package org.ergoplatform.uexplorer.indexer.cassandra

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql._

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait CassandraPersistenceSupport {

  protected[cassandra] val node_epochs_table = "node_epochs"

  protected[cassandra] val epoch_index    = "epoch_index"
  protected[cassandra] val last_header_id = "last_header_id"

  def storeBlockFlow[T](
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

  def storeBlockBatchFlow[T](
    parallelism: Int,
    batchType: BatchType = DefaultBatchType.LOGGED,
    simpleStatement: SimpleStatement,
    statementBinder: (T, PreparedStatement) => List[BoundStatement]
  )(implicit cqlSession: CqlSession): Flow[T, T, NotUsed] =
    Flow
      .lazyFutureFlow(() =>
        cqlSession.prepareAsync(simpleStatement).toScala.map { preparedStatement =>
          Flow[T]
            .mapAsync(parallelism) { element =>
              statementBinder(element, preparedStatement) match {
                case Nil =>
                  Future.successful(element)
                case statement :: Nil =>
                  cqlSession
                    .executeAsync(statement)
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
