package org.ergoplatform.uexplorer.indexer.scylla.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.{bindMarker, insertInto}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.Const
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor._
import org.ergoplatform.uexplorer.indexer.progress.{Epoch, InvalidEpochCandidate}
import org.ergoplatform.uexplorer.indexer.scylla.{ScyllaBackend, ScyllaPersistenceSupport}

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait ScyllaEpochWriter extends LazyLogging {
  this: ScyllaBackend =>
  import ScyllaEpochWriter._

  def epochWriteFlow: Flow[MaybeNewEpoch, Either[Int, Epoch], NotUsed] =
    Flow[MaybeNewEpoch]
      .mapAsync(1) {
        case NewEpochCreated(epoch) =>
          persistEpoch(epoch).map(Right(_))
        case NewEpochFailed(InvalidEpochCandidate(epochIndex, invalidHeightsAsc, error)) =>
          logger.error(s"Epoch $epochIndex is invalid due to $error at heights ${invalidHeightsAsc.mkString(",")}")
          Future.successful(Left(epochIndex))
        case NewEpochExisted(epochIndex) =>
          logger.debug(s"Skipping persistence of epoch $epochIndex as it already existed")
          Future.successful(Left(epochIndex))
      }

  def persistEpoch(epoch: Epoch): Future[Epoch] =
    cqlSession
      .prepareAsync(epochInsertStatement)
      .toScala
      .map(epochInsertBinder(epoch))
      .flatMap { stmnt =>
        cqlSession
          .executeAsync(stmnt)
          .toScala
          .map(_ => epoch)
      }
}

object ScyllaEpochWriter extends ScyllaPersistenceSupport {

  protected[scylla] def epochInsertBinder(epoch: Epoch)(stmt: PreparedStatement): BoundStatement =
    stmt
      .bind()
      .setInt(epoch_index, epoch.index)
      .setString(last_header_id, epoch.blockIds.last.value.unwrapped)

  protected[scylla] val epochInsertStatement: SimpleStatement =
    insertInto(Const.ScyllaKeyspace, node_epochs_table)
      .value(epoch_index, bindMarker(epoch_index))
      .value(last_header_id, bindMarker(last_header_id))
      .build()
      .setIdempotent(true)

}
