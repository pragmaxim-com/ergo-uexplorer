package org.ergoplatform.uexplorer.indexer.cassandra.entity

import akka.NotUsed

import scala.jdk.CollectionConverters.*
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, BoundStatement, PreparedStatement, Row, SimpleStatement}
import com.datastax.oss.driver.api.core.data.TupleValue
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.{MapPimp, MutableMapPimp, UnexpectedStateError}
import org.ergoplatform.uexplorer.indexer.cassandra.{CassandraBackend, CassandraPersistenceSupport, EpochPersistenceSupport}
import org.ergoplatform.uexplorer.indexer.chain.ChainState.BufferedBlockInfo
import org.ergoplatform.uexplorer.{db, indexer, Address, BlockId, BoxId, Const, Height, TxId, TxIndex}

import scala.collection.immutable.{ArraySeq, TreeMap, TreeSet}
import scala.jdk.FutureConverters.*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.indexer.chain.{ChainState, Epoch}
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState.Tx

import scala.collection.compat.immutable.ArraySeq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*

trait CassandraUtxoReader extends EpochPersistenceSupport with LazyLogging {
  this: CassandraBackend =>

  import CassandraUtxoReader._

  private lazy val headerSelectWhereHeightTry  = Try(cqlSession.prepare(headerSelectStatement))
  private lazy val outputsSelectWhereHeaderTry = Try(cqlSession.prepare(outputsSelectStatement))

  private lazy val inputBoxIdSelectTry =
    Try(cqlSession.prepare(inputBoxIdSelectStatement)).map(_ -> cqlSession.prepare(outputAddressValueSelectStatement))

  private def getHeaderByHeight(height: Int): Future[(Int, Option[(String, Long)])] =
    Future.fromTry(headerSelectWhereHeightTry).flatMap { headerSelectWhereHeight =>
      cqlSession
        .executeAsync(headerSelectWhereHeight.bind(height))
        .asScala
        .map(rs => height -> Option(rs.one()).map(r => r.getString(Headers.header_id) -> r.getLong(Headers.timestamp)))
    }

  private def getBoxesByTx(
    inputsByTxId: Map[TxId, ArraySeq[(BoxId, Address, Long)]],
    headerId: String,
    height: Int,
    timestamp: Long
  ) =
    Future.fromTry(outputsSelectWhereHeaderTry).flatMap { outputsSelectWhereHeader =>
      Source
        .fromPublisher(cqlSession.executeReactive(outputsSelectWhereHeader.bind(headerId)))
        .buffer(Const.EpochLength * 2, OverflowStrategy.backpressure)
        .runFold(Map.empty[(TxId, TxIndex), ArraySeq[(BoxId, Address, Long)]]) { case (acc, r) =>
          val outputs = (BoxId(r.getString(box_id)), Address.fromStringUnsafe(r.getString(address)), r.getLong(value))
          acc.adjust(TxId(r.getString(tx_id)) -> r.getShort(tx_idx)) {
            case None =>
              ArraySeq(outputs)
            case Some(outs) =>
              outs :+ outputs
          }
        }
        .map { outputsByTx =>
          outputsByTx.toSeq.sortBy(_._1._2).map { case ((txId, txIndex), outputs) =>
            val inputs =
              if (txId == Const.Genesis.Emission.tx) {
                ArraySeq(
                  (Const.Genesis.Emission.box, Const.Genesis.Emission.address, Const.Genesis.Emission.initialNanoErgs)
                )
              } else if (txId == Const.Genesis.Foundation.tx) {
                ArraySeq(
                  (
                    Const.Genesis.Foundation.box,
                    Const.Genesis.Foundation.address,
                    Const.Genesis.Foundation.initialNanoErgs
                  )
                )
              } else {
                inputsByTxId
                  .getOrElse(
                    txId,
                    throw new IllegalStateException(s"Tx $txId from height $height and header $headerId not found")
                  )
              }
            (Tx(txId, txIndex, height, timestamp), (inputs, outputs))
          }
        }
    }

  private def getInputs(headerId: String) =
    Future.fromTry(inputBoxIdSelectTry).flatMap { case (inputBoxIdSelectWhereHeader, outputAddressValueSelectWhereHeader) =>
      Source
        .fromPublisher(cqlSession.executeReactive(inputBoxIdSelectWhereHeader.bind(headerId)))
        .map(r => TxId(r.getString(tx_id)) -> r.getString(box_id))
        .buffer(Const.EpochLength * 2, OverflowStrategy.backpressure)
        .mapAsync(8) { case (txId, boxId) =>
          cqlSession.executeAsync(outputAddressValueSelectWhereHeader.bind(boxId)).asScala.map { r =>
            Option(r.one())
              .map(r =>
                (
                  txId,
                  BoxId(r.getString(box_id)),
                  Address.fromStringUnsafe(r.getString(address)),
                  r.getLong(value)
                )
              )
          }
        }
        .mapConcat(_.toList)
        .buffer(Const.EpochLength * 2, OverflowStrategy.backpressure)
        .runFold(Map.empty[TxId, ArraySeq[(BoxId, Address, Long)]]) { case (acc, (txId, boxId, address, value)) =>
          acc.adjust(txId)(_.fold(ArraySeq((boxId, address, value)))(xs => xs :+ (boxId, address, value)))
        }
    }

  def transactionBoxesByHeightFlow: Flow[Height, (Height, UtxoState.BoxesByTx), NotUsed] =
    Flow[Height]
      .mapAsync(1)(getHeaderByHeight)
      .mapAsync(1) {
        case (height, None) =>
          Future.failed(new UnexpectedStateError(s"There is no header for height $height"))
        case (height, Some((headerId, timestamp))) =>
          getInputs(headerId).map(inputs => (inputs, headerId, height, timestamp))
      }
      .mapAsync(8) { case (inputs, headerId, height, timestamp) =>
        getBoxesByTx(inputs, headerId, height, timestamp).map(boxesByTx => height -> boxesByTx)
      }
      .buffer(32, OverflowStrategy.backpressure)

}

object CassandraUtxoReader extends CassandraPersistenceSupport {

  protected[cassandra] def headerSelectStatement: SimpleStatement =
    QueryBuilder
      .selectFrom(indexer.Const.CassandraKeyspace, Headers.node_headers_table)
      .columns(Headers.header_id, Headers.timestamp)
      .whereColumn(Headers.height)
      .isEqualTo(QueryBuilder.bindMarker(Headers.height))
      .build()

  protected[cassandra] def outputsSelectStatement: SimpleStatement =
    QueryBuilder
      .selectFrom(indexer.Const.CassandraKeyspace, Outputs.node_outputs_table)
      .columns(Outputs.tx_id, Outputs.tx_idx, Outputs.box_id, Outputs.address, Outputs.value)
      .whereColumn(Outputs.header_id)
      .isEqualTo(QueryBuilder.bindMarker(Outputs.header_id))
      .build()

  protected[cassandra] def inputBoxIdSelectStatement: SimpleStatement =
    QueryBuilder
      .selectFrom(indexer.Const.CassandraKeyspace, Inputs.node_inputs_table)
      .columns(Inputs.tx_id, Inputs.box_id)
      .whereColumn(Inputs.header_id)
      .isEqualTo(QueryBuilder.bindMarker(Inputs.header_id))
      .build()

  protected[cassandra] def outputAddressValueSelectStatement: SimpleStatement =
    QueryBuilder
      .selectFrom(indexer.Const.CassandraKeyspace, Outputs.node_outputs_table)
      .columns(Outputs.box_id, Outputs.address, Outputs.value)
      .whereColumn(Outputs.box_id)
      .isEqualTo(QueryBuilder.bindMarker(Outputs.box_id))
      .build()
}
