package org.ergoplatform.uexplorer.indexer.cassandra.entity

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, BoundStatement, PreparedStatement, Row, SimpleStatement}
import com.datastax.oss.driver.api.core.data.TupleValue
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.Const
import org.ergoplatform.uexplorer.indexer.cassandra.{CassandraBackend, CassandraPersistenceSupport, EpochPersistenceSupport}
import org.ergoplatform.uexplorer.indexer.chain.ChainState.BufferedBlockInfo
import org.ergoplatform.uexplorer.{db, Address, BlockId, BoxId}

import scala.collection.immutable.{ArraySeq, TreeMap, TreeSet}
import scala.compat.java8.FutureConverters.*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}
import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.indexer.chain.{ChainState, Epoch}
import org.ergoplatform.uexplorer.indexer.MutableMapPimp
import org.ergoplatform.uexplorer.indexer.MapPimp
import org.ergoplatform.uexplorer.indexer.utxo.{UtxoSnapshot, UtxoState}

import scala.collection.compat.immutable.ArraySeq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*

trait CassandraUtxoReader extends EpochPersistenceSupport with LazyLogging {
  this: CassandraBackend =>

  import CassandraUtxoReader._

  private lazy val headerSelectWhereHeight  = cqlSession.prepare(headerSelectStatement)
  private lazy val outputsSelectWhereHeader = cqlSession.prepare(outputsSelectStatement)
  private lazy val inputsSelectWhereHeader  = cqlSession.prepare(inputsSelectStatement)

  private def getHeaderByHeight(height: Int): Future[(Int, String)] =
    cqlSession
      .executeAsync(headerSelectWhereHeight.bind(height))
      .toScala
      .map(rs => height -> rs.one().getString(Headers.header_id))

  private def getOutputs(headerId: String) =
    Source
      .fromPublisher(cqlSession.executeReactive(outputsSelectWhereHeader.bind(headerId)))
      .runFold(ArraySeq.newBuilder[(BoxId, Address, Long)]) { case (acc, r) =>
        acc.addOne(
          (BoxId(r.getString(box_id)), Address.fromStringUnsafe(r.getString(address)), r.getLong(value))
        )
      }

  private def getInputs(headerId: String) =
    Source
      .fromPublisher(cqlSession.executeReactive(inputsSelectWhereHeader.bind(headerId)))
      .runFold(ArraySeq.newBuilder[BoxId]) { case (acc, r) =>
        acc.addOne(BoxId(r.getString(box_id)))
      }

  def loadUtxoState(epochIndexes: Iterator[Int]): Future[UtxoState] =
    if (!epochIndexes.hasNext) {
      logger.info(s"Creating new UtxoState as db is empty")
      Future.successful(UtxoState.empty)
    } else {
      logger.info(s"Loading utxo state from database")
      Source
        .fromIterator(() => epochIndexes)
        .mapConcat(Epoch.heightRangeForEpochIndex)
        .mapAsync(1)(getHeaderByHeight)
        .mapAsync(2) { case (height, headerId) =>
          val outputsF = getOutputs(headerId)
          getInputs(headerId)
            .flatMap(inputs => outputsF.map(outputs => (height, (inputs.result(), outputs.result()))))
        }
        .buffer(32, OverflowStrategy.backpressure)
        .grouped(Const.EpochLength)
        .runFold(UtxoState.empty) { case (s, boxesByHeight) =>
          val epochIndex = Epoch.epochIndexForHeight(boxesByHeight.head._1)
          logger.info(s"Merging boxes of epoch $epochIndex into utxo state")
          s.mergeEpochFromBuffer(boxesByHeight.iterator)
        }
    }
}

object CassandraUtxoReader extends CassandraPersistenceSupport {

  protected[cassandra] val headerSelectStatement: SimpleStatement =
    QueryBuilder
      .selectFrom(Const.CassandraKeyspace, Headers.node_headers_table)
      .columns(Headers.header_id)
      .whereColumn(Headers.height)
      .isEqualTo(QueryBuilder.bindMarker(Headers.height))
      .build()

  protected[cassandra] val outputsSelectStatement: SimpleStatement =
    QueryBuilder
      .selectFrom(Const.CassandraKeyspace, Outputs.node_outputs_table)
      .columns(Outputs.box_id, Outputs.address, Outputs.value)
      .whereColumn(Outputs.header_id)
      .isEqualTo(QueryBuilder.bindMarker(Outputs.header_id))
      .build()

  protected[cassandra] val inputsSelectStatement: SimpleStatement =
    QueryBuilder
      .selectFrom(Const.CassandraKeyspace, Inputs.node_inputs_table)
      .columns(Outputs.box_id)
      .whereColumn(Inputs.header_id)
      .isEqualTo(QueryBuilder.bindMarker(Inputs.header_id))
      .build()

}
