package org.ergoplatform.uexplorer.indexer.cassandra.entity

import scala.jdk.CollectionConverters.*
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, BoundStatement, PreparedStatement, Row, SimpleStatement}
import com.datastax.oss.driver.api.core.data.TupleValue
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.{MapPimp, MutableMapPimp, UnexpectedStateError}
import org.ergoplatform.uexplorer.indexer.cassandra.{CassandraBackend, CassandraPersistenceSupport, EpochPersistenceSupport}
import org.ergoplatform.uexplorer.indexer.chain.ChainState.BufferedBlockInfo
import org.ergoplatform.uexplorer.{db, indexer, Address, BlockId, BoxId, Const}

import scala.collection.immutable.{ArraySeq, TreeMap, TreeSet}
import scala.jdk.FutureConverters.*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.indexer.chain.{ChainState, Epoch}
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState

import scala.collection.compat.immutable.ArraySeq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*

trait CassandraUtxoReader extends EpochPersistenceSupport with LazyLogging {
  this: CassandraBackend =>

  import CassandraUtxoReader._

  private lazy val headerSelectWhereHeightTry  = Try(cqlSession.prepare(headerSelectStatement))
  private lazy val outputsSelectWhereHeaderTry = Try(cqlSession.prepare(outputsSelectStatement))

  private lazy val inputBoxIdSelectWhereHeaderTry =
    Try(cqlSession.prepare(inputBoxIdSelectStatement)).map(_ -> cqlSession.prepare(inputAddressValueSelectStatement))

  private def getHeaderByHeight(height: Int): Future[(Int, Option[String])] =
    Future.fromTry(headerSelectWhereHeightTry).flatMap { headerSelectWhereHeight =>
      cqlSession
        .executeAsync(headerSelectWhereHeight.bind(height))
        .asScala
        .map(rs => height -> Option(rs.one()).map(_.getString(Headers.header_id)))
    }

  private def getOutputs(headerId: String) =
    Future.fromTry(outputsSelectWhereHeaderTry).flatMap { outputsSelectWhereHeader =>
      Source
        .fromPublisher(cqlSession.executeReactive(outputsSelectWhereHeader.bind(headerId)))
        .runFold(ArraySeq.newBuilder[(BoxId, Address, Long)]) { case (acc, r) =>
          acc.addOne(
            (BoxId(r.getString(box_id)), Address.fromStringUnsafe(r.getString(address)), r.getLong(value))
          )
        }
    }

  private def getInputs(headerId: String) =
    Future.fromTry(inputBoxIdSelectWhereHeaderTry).flatMap {
      case (inputBoxIdSelectWhereHeader, inputAddressValueSelectWhereHeader) =>
        Source
          .fromPublisher(cqlSession.executeReactive(inputBoxIdSelectWhereHeader.bind(headerId)))
          .map(_.getString(box_id))
          .mapAsync(16) { boxId =>
            cqlSession.executeAsync(inputAddressValueSelectWhereHeader.bind(boxId)).asScala.map { r =>
              Option(r.one())
                .map(r => (BoxId(r.getString(box_id)), Address.fromStringUnsafe(r.getString(address)), r.getLong(value)))
            }
          }
          .mapConcat(_.toList)
          .runFold(ArraySeq.newBuilder[(BoxId, Address, Long)]) { case (acc, r) =>
            acc.addOne(r)
          }
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
        .mapAsync(1) {
          case (height, None) =>
            Future.failed(new UnexpectedStateError(s"There is no header for height $height"))
          case (height, Some(headerId)) =>
            val outputsF = getOutputs(headerId)
            val inputsF  = getInputs(headerId)
            for {
              inputs  <- inputsF
              outputs <- outputsF
            } yield (height, (inputs.result(), outputs.result()))
        }
        .buffer(32, OverflowStrategy.backpressure)
        .grouped(Const.EpochLength)
        .runFold(UtxoState.empty) { case (s, boxesByHeight) =>
          val epochIndex = Epoch.epochIndexForHeight(boxesByHeight.head._1)
          logger.info(s"Merging boxes of epoch $epochIndex into utxo state")
          s.mergeGivenBoxes(boxesByHeight.iterator.map(_._2))
        }
    }
}

object CassandraUtxoReader extends CassandraPersistenceSupport {

  protected[cassandra] def headerSelectStatement: SimpleStatement =
    QueryBuilder
      .selectFrom(indexer.Const.CassandraKeyspace, Headers.node_headers_table)
      .columns(Headers.header_id)
      .whereColumn(Headers.height)
      .isEqualTo(QueryBuilder.bindMarker(Headers.height))
      .build()

  protected[cassandra] def outputsSelectStatement: SimpleStatement =
    QueryBuilder
      .selectFrom(indexer.Const.CassandraKeyspace, Outputs.node_outputs_table)
      .columns(Outputs.box_id, Outputs.address, Outputs.value)
      .whereColumn(Outputs.header_id)
      .isEqualTo(QueryBuilder.bindMarker(Outputs.header_id))
      .build()

  protected[cassandra] def inputBoxIdSelectStatement: SimpleStatement =
    QueryBuilder
      .selectFrom(indexer.Const.CassandraKeyspace, Inputs.node_inputs_table)
      .columns(Inputs.box_id)
      .whereColumn(Inputs.header_id)
      .isEqualTo(QueryBuilder.bindMarker(Inputs.header_id))
      .build()

  protected[cassandra] def inputAddressValueSelectStatement: SimpleStatement =
    QueryBuilder
      .selectFrom(indexer.Const.CassandraKeyspace, Outputs.node_outputs_table)
      .columns(Outputs.box_id, Outputs.address, Outputs.value)
      .whereColumn(Outputs.box_id)
      .isEqualTo(QueryBuilder.bindMarker(Outputs.box_id))
      .build()
}
