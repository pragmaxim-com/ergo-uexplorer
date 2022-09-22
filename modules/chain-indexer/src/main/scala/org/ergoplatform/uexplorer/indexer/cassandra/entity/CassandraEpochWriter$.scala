package org.ergoplatform.uexplorer.indexer.cassandra.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.{bindMarker, insertInto}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.uexplorer.indexer.Const
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor._
import org.ergoplatform.uexplorer.indexer.progress.{Epoch, InvalidEpochCandidate}
import org.ergoplatform.uexplorer.indexer.cassandra.{CassandraBackend, CassandraPersistenceSupport}

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait CassandraEpochWriter$ extends LazyLogging {
  this: CassandraBackend =>
  import CassandraEpochWriter$._

  def epochWriteFlow: Flow[(FlatBlock, Option[MaybeNewEpoch]), (FlatBlock, Option[MaybeNewEpoch]), NotUsed] =
    Flow[(FlatBlock, Option[MaybeNewEpoch])]
      .mapAsync(1) {
        case (block, s @ Some(NewEpochCreated(epoch))) =>
          persistEpoch(epoch).map(_ => block -> s)
        case (block, s @ Some(NewEpochFailed(InvalidEpochCandidate(epochIndex, invalidHeightsAsc, error)))) =>
          logger.error(s"Epoch $epochIndex is invalid due to $error at heights ${invalidHeightsAsc.mkString(",")}")
          Future.successful(block -> s)
        case (block, s @ Some(NewEpochExisted(epochIndex))) =>
          logger.debug(s"Skipping persistence of epoch $epochIndex as it already existed")
          Future.successful(block -> s)
        case t =>
          Future.successful(t)
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

object CassandraEpochWriter$ extends CassandraPersistenceSupport {

  protected[cassandra] def epochInsertBinder(epoch: Epoch)(stmt: PreparedStatement): BoundStatement =
    stmt
      .bind()
      .setInt(epoch_index, epoch.index)
      .setString(last_header_id, epoch.blockIds.last.value.unwrapped)

  protected[cassandra] val epochInsertStatement: SimpleStatement =
    insertInto(Const.CassandraKeyspace, node_epochs_table)
      .value(epoch_index, bindMarker(epoch_index))
      .value(last_header_id, bindMarker(last_header_id))
      .build()
      .setIdempotent(true)

}
