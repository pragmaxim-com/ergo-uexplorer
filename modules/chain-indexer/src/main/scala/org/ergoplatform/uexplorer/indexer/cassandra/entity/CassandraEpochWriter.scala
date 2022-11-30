package org.ergoplatform.uexplorer.indexer.cassandra.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.core.cql.{BoundStatement, DefaultBatchType, PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.data.TupleValue
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.{bindMarker, insertInto}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.Const
import org.ergoplatform.uexplorer.indexer.cassandra.{CassandraBackend, EpochPersistenceSupport}
import org.ergoplatform.uexplorer.indexer.chain.ChainSyncer.*
import org.ergoplatform.uexplorer.indexer.chain.{Epoch, InvalidEpochCandidate}

import scala.compat.java8.FutureConverters.*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.Address

import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters.*

trait CassandraEpochWriter extends LazyLogging {
  this: CassandraBackend =>
  import CassandraEpochWriter._

  def epochsWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed] =
    storeBatchFlow(
      parallelism = 1,
      batchType   = DefaultBatchType.LOGGED,
      buildInsertStatement(List(epoch_index, last_header_id), node_epoch_last_headers_table),
      epochLastHeadersInsertBinder
    ).via(
      storeBatchFlow(
        parallelism = 1,
        batchType   = DefaultBatchType.LOGGED,
        buildInsertStatement(List(epoch_index, box_id), node_epochs_inputs_table),
        epochInputsInsertBinder
      )
    ).via(
      storeBatchFlow(
        parallelism = 1,
        batchType   = DefaultBatchType.LOGGED,
        buildInsertStatement(List(epoch_index, address, box_id, value), node_epochs_outputs_table),
        epochOutputsInsertBinder
      )
    )
}

object CassandraEpochWriter extends EpochPersistenceSupport with LazyLogging {

  protected[cassandra] def epochLastHeadersInsertBinder
    : ((Block, Option[MaybeNewEpoch]), PreparedStatement) => ArraySeq[BoundStatement] = {
    case ((_, Some(NewEpochCreated(epoch))), stmt) =>
      ArraySeq(
        stmt
          .bind()
          .setInt(epoch_index, epoch.index)
          .setString(last_header_id, epoch.blockIds.last)
      )
    case ((_, Some(NewEpochFailed(InvalidEpochCandidate(epochIndex, invalidHeightsAsc, error)))), _) =>
      logger.error(s"Epoch $epochIndex is invalid due to $error at heights ${invalidHeightsAsc.mkString(",")}")
      ArraySeq.empty
    case ((_, Some(NewEpochExisted(epochIndex))), _) =>
      logger.debug(s"Skipping persistence of epoch $epochIndex as it already existed")
      ArraySeq.empty
    case _ =>
      ArraySeq.empty
  }

  protected[cassandra] def epochInputsInsertBinder
    : ((Block, Option[MaybeNewEpoch]), PreparedStatement) => ArraySeq[BoundStatement] = {
    case ((_, Some(NewEpochCreated(epoch))), stmt) =>
      epoch.inputIds.map { boxId =>
        stmt
          .bind()
          .setInt(epoch_index, epoch.index)
          .setString(box_id, boxId.unwrapped)
      }
    case _ =>
      ArraySeq.empty
  }

  protected[cassandra] def epochOutputsInsertBinder
    : ((Block, Option[MaybeNewEpoch]), PreparedStatement) => ArraySeq[BoundStatement] = {
    case ((_, Some(NewEpochCreated(epoch))), stmt) =>
      ArraySeq.from(
        epoch.utxosByAddress.flatMap { case (addr, valueByBox) =>
          valueByBox.map { case (boxId, v) =>
            stmt
              .bind()
              .setInt(epoch_index, epoch.index)
              .setString(address, addr)
              .setString(box_id, boxId.unwrapped)
              .setLong(value, v)
          }
        }.toArray
      )
    case _ =>
      ArraySeq.empty
  }

}
