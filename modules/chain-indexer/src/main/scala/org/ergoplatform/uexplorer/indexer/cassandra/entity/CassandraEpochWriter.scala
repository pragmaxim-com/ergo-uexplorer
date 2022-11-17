package org.ergoplatform.uexplorer.indexer.cassandra.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.data.TupleValue
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.{bindMarker, insertInto}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.Const
import org.ergoplatform.uexplorer.indexer.cassandra.{CassandraBackend, EpochPersistenceSupport}
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor.*
import org.ergoplatform.uexplorer.indexer.progress.{Epoch, InvalidEpochCandidate}

import scala.compat.java8.FutureConverters.*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.Address

import scala.jdk.CollectionConverters.*

trait CassandraEpochWriter extends LazyLogging {
  this: CassandraBackend =>
  import CassandraEpochWriter._

  def epochWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed] =
    Flow[(Block, Option[MaybeNewEpoch])]
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

object CassandraEpochWriter extends EpochPersistenceSupport {

  protected[cassandra] def epochInsertBinder(epoch: Epoch)(stmt: PreparedStatement): BoundStatement = {
    val tupleType = DataTypes.tupleOf(DataTypes.TEXT, DataTypes.TEXT)
    stmt
      .bind()
      .setInt(epoch_index, epoch.index)
      .setString(last_header_id, epoch.blockIds.last)
      .setList(input_box_ids, epoch.inputIds.map(_.unwrapped).asJava, classOf[String])
      .setList(
        output_box_ids_with_address,
        epoch.addressByOutputIds.map { case (boxId, address) =>
          tupleType.newValue(boxId.unwrapped, address.toString)
        }.asJava,
        classOf[TupleValue]
      )
  }

  protected[cassandra] val epochInsertStatement: SimpleStatement =
    insertInto(Const.CassandraKeyspace, node_epochs_table)
      .value(epoch_index, bindMarker(epoch_index))
      .value(last_header_id, bindMarker(last_header_id))
      .value(input_box_ids, bindMarker(input_box_ids))
      .value(output_box_ids_with_address, bindMarker(output_box_ids_with_address))
      .build()
      .setIdempotent(true)

}
