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
import org.ergoplatform.uexplorer.indexer.chain.{ChainState, Epoch, UtxoState}
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
        if (infoByIndex.isEmpty) {
          Future.successful(ChainState.load(infoByIndex, UtxoState.empty))
        } else {
          val heightRangeForAllEpochs = infoByIndex.iterator.flatMap(i => Epoch.heightRangeForEpochIndex(i._1)).toIndexedSeq
          logger.info(
            s"Loading ${infoByIndex.size} epochs from ${infoByIndex.head._1} to ${infoByIndex.last._1} from database"
          )
          Source(heightRangeForAllEpochs)
            .mapAsync(1) { height =>
              cqlSession
                .executeAsync(
                  s"SELECT ${Headers.header_id} FROM ${Const.CassandraKeyspace}.${Headers.node_headers_table} WHERE ${Headers.height} = $height;"
                )
                .toScala
                .map(rs => height -> rs.one().getString(Headers.header_id))
            }
            .mapAsync(2) { case (height, headerId) =>
              val outputsF =
                Source
                  .fromPublisher(
                    cqlSession.executeReactive(
                      s"SELECT $box_id, $address, $value FROM ${Const.CassandraKeyspace}.${Outputs.node_outputs_table} WHERE ${Outputs.header_id} = '$headerId';"
                    )
                  )
                  .runFold(ArraySeq.newBuilder[(BoxId, Address, Long)]) { case (acc, r) =>
                    acc.addOne(
                      (BoxId(r.getString(box_id)), Address.fromStringUnsafe(r.getString(address)), r.getLong(value))
                    )
                  }
              val inputsF =
                Source
                  .fromPublisher(
                    cqlSession.executeReactive(
                      s"SELECT $box_id FROM ${Const.CassandraKeyspace}.${Inputs.node_inputs_table} WHERE ${Outputs.header_id} = '$headerId';"
                    )
                  )
                  .runFold(ArraySeq.newBuilder[BoxId]) { case (acc, r) =>
                    acc.addOne(BoxId(r.getString(box_id)))
                  }
              inputsF.flatMap(inputs => outputsF.map(outputs => (height, inputs.result(), outputs.result())))
            }
            .buffer(16, OverflowStrategy.backpressure)
            .runFold(UtxoState.empty) { case (s, (height, inputs, outputs)) =>
              s.bufferBestBlock(height, inputs, outputs)
            }
            .map { utxoState =>
              ChainState.load(infoByIndex, utxoState.mergeEpochFromBuffer(heightRangeForAllEpochs))
            }
        }
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
