package org.ergoplatform.uexplorer.indexer.cassandra.entity

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
import org.ergoplatform.uexplorer.indexer.chain.{ChainState, UtxoState}
import org.ergoplatform.uexplorer.indexer.MutableMapPimp
import org.ergoplatform.uexplorer.indexer.MapPimp

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*

trait CassandraEpochReader extends EpochPersistenceSupport with LazyLogging {
  this: CassandraBackend =>

  import CassandraEpochReader._

  private val blockInfoSelectPreparedStatement = cqlSession.prepare(blockInfoSelectStatement)

  private def blockInfoByEpochIndex(
    epochIndex: Int,
    headerId: String
  ): Future[(Int, BufferedBlockInfo)] =
    cqlSession
      .executeAsync(blockInfoSelectBinder(blockInfoSelectPreparedStatement)(BlockId.fromStringUnsafe(headerId)))
      .toScala
      .map(_.one())
      .map(r => epochIndex -> blockInfoRowReader(r))

  def getCachedState: Future[ChainState] = {
    logger.info(s"Loading epoch indexes ...")
    Source
      .fromPublisher(
        cqlSession.executeReactive(
          s"SELECT $epoch_index, $last_header_id FROM ${Const.CassandraKeyspace}.$node_epoch_last_headers_table;"
        )
      )
      .mapAsync(1)(row => blockInfoByEpochIndex(row.getInt(epoch_index), row.getString(last_header_id)))
      .runWith(Sink.seq[(Int, BufferedBlockInfo)])
      .map(infoByIndex => TreeMap(infoByIndex: _*))
      .flatMap { infoByIndex =>
        val rangeOpt = infoByIndex.headOption.map(head => s": from ${head._1} to ${infoByIndex.last._1}").getOrElse("")
        logger.info(s"Loading ${infoByIndex.size} epochs $rangeOpt from database")
        Source(infoByIndex)
          .mapAsync(1) { case (epochIndex, _) =>
            Source
              .fromPublisher(
                cqlSession
                  .executeReactive(
                    s"SELECT $epoch_index, $box_id FROM ${Const.CassandraKeyspace}.$node_epochs_inputs_table WHERE $epoch_index = $epochIndex;"
                  )
              )
              .runFold(ArrayBuffer.newBuilder[BoxId]) { case (acc, r) => acc.addOne(BoxId(r.getString(box_id))) }
              .map(r => epochIndex -> ArraySeq.from(r.result().toArray))
          }
          .mapAsync(1) { case (epochIndex, inputIds) =>
            Source
              .fromPublisher(
                cqlSession
                  .executeReactive(
                    s"SELECT $epoch_index, $address, $box_id, $value FROM ${Const.CassandraKeyspace}.$node_epochs_outputs_table WHERE $epoch_index = $epochIndex;"
                  )
              )
              .runFold(Map.empty[Address, mutable.Map[BoxId, Long]]) { case (acc, r) =>
                val addr  = Address.fromStringUnsafe(r.getString(address))
                val boxId = BoxId(r.getString(box_id))
                val v     = r.getLong(value)
                acc.adjust(addr)(_.fold(mutable.Map(boxId -> v)) { x => x.put(boxId, v); x })
              }
              .map(outputIds => (epochIndex, inputIds, outputIds))
          }
          .runFold(UtxoState.empty) { case (s, (epochIdx, inputs, outputs)) =>
            logger.info(s"$epochIdx: inputBoxes[${inputs.size}], utxo-addresses[${outputs.size}]")
            s.mergeEpochFromBoxes(inputs, outputs)
          }
          .map(utxoState => ChainState.load(infoByIndex, utxoState))
      }
  }
}

object CassandraEpochReader extends CassandraPersistenceSupport {

  protected[cassandra] val blockInfoSelectStatement: SimpleStatement =
    QueryBuilder
      .selectFrom(Const.CassandraKeyspace, Headers.node_headers_table)
      .columns(
        Headers.header_id,
        Headers.parent_id,
        Headers.timestamp,
        Headers.height,
        Headers.main_chain,
        Headers.BlockInfo.udtName
      )
      .whereColumn(Headers.header_id)
      .isEqualTo(QueryBuilder.bindMarker(Headers.header_id))
      .build()

  protected[cassandra] def blockInfoSelectBinder(preparedStatement: PreparedStatement)(headerId: BlockId): BoundStatement =
    preparedStatement.bind().setString(Headers.header_id, headerId)

  protected[cassandra] def blockInfoRowReader(row: Row): BufferedBlockInfo = {
    import Headers.BlockInfo._
    val blockInfoUdt = row.getUdtValue(Headers.BlockInfo.udtName)
    BufferedBlockInfo(
      BlockId.fromStringUnsafe(row.getString(Headers.header_id)),
      BlockId.fromStringUnsafe(row.getString(Headers.parent_id)),
      row.getLong(Headers.timestamp),
      row.getInt(Headers.height),
      db.BlockInfo(
        blockInfoUdt.getInt(block_size),
        blockInfoUdt.getLong(block_coins),
        Option(blockInfoUdt.getLong(block_mining_time)),
        blockInfoUdt.getInt(txs_count),
        blockInfoUdt.getInt(txs_size),
        Address.fromStringUnsafe(blockInfoUdt.getString(miner_address)),
        blockInfoUdt.getLong(miner_reward),
        blockInfoUdt.getLong(miner_revenue),
        blockInfoUdt.getLong(block_fee),
        blockInfoUdt.getLong(block_chain_total_size),
        blockInfoUdt.getLong(total_txs_count),
        blockInfoUdt.getLong(total_coins_issued),
        blockInfoUdt.getLong(total_mining_time),
        blockInfoUdt.getLong(total_fees),
        blockInfoUdt.getLong(total_miners_reward),
        blockInfoUdt.getLong(total_coins_in_txs),
        blockInfoUdt.getLong(max_tx_gix),
        blockInfoUdt.getLong(max_box_gix)
      )
    )
  }

}
