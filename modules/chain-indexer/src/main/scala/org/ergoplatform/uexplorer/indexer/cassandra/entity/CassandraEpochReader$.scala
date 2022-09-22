package org.ergoplatform.uexplorer.indexer.cassandra.entity

import akka.stream.scaladsl.{Sink, Source}
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement, Row, SimpleStatement}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.db.models.BlockStats
import org.ergoplatform.explorer.{Address, BlockId}
import org.ergoplatform.uexplorer.indexer.Const
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient.BlockInfo
import org.ergoplatform.uexplorer.indexer.cassandra.{CassandraBackend, CassandraPersistenceSupport}

import scala.collection.immutable.TreeMap
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

trait CassandraEpochReader$ extends LazyLogging {
  this: CassandraBackend =>

  import CassandraEpochReader$._

  private val blockInfoSelectPreparedStatement = cqlSession.prepare(blockInfoSelectStatement)

  def getBlockInfo(headerId: BlockId): Future[BlockInfo] =
    cqlSession
      .executeAsync(blockInfoSelectBinder(blockInfoSelectPreparedStatement)(headerId))
      .toScala
      .map(_.one())
      .map(blockInfoRowReader)

  def getLastBlockInfoByEpochIndex: Future[TreeMap[Int, BlockInfo]] = {
    logger.debug(s"Getting existing epoch indexes from db ")
    Source
      .fromPublisher(
        cqlSession.executeReactive(s"SELECT $last_header_id, $epoch_index FROM ${Const.CassandraKeyspace}.$node_epochs_table;")
      )
      .mapAsync(1)(r => getBlockInfo(BlockId.fromStringUnsafe(r.getString(last_header_id))).map(r.getInt(epoch_index) -> _))
      .runWith(Sink.seq)
      .map(indexes => TreeMap(indexes: _*))
      .andThen {
        case Failure(ex) => logger.error(s"Getting epoch indexes failed", ex)
        case Success(indexes) =>
          val rangeOpt = indexes.headOption.map(head => s": from $head to ${indexes.last}").getOrElse("")
          logger.debug(s"Db contains ${indexes.size} epochs $rangeOpt")
      }
  }
}

object CassandraEpochReader$ extends CassandraPersistenceSupport {

  protected[cassandra] val blockInfoSelectStatement: SimpleStatement = {
    import BlocksInfo._
    QueryBuilder
      .selectFrom(Const.CassandraKeyspace, block_info_table)
      .columns(columns: _*)
      .whereColumn(header_id)
      .isEqualTo(QueryBuilder.bindMarker(header_id))
      .build()
  }

  protected[cassandra] def blockInfoSelectBinder(preparedStatement: PreparedStatement)(headerId: BlockId): BoundStatement =
    preparedStatement.bind().setString(BlocksInfo.header_id, headerId.value.unwrapped)

  protected[cassandra] def blockInfoRowReader(row: Row): BlockInfo = {
    import BlocksInfo._
    BlockInfo(
      BlockId.fromStringUnsafe(row.getString(parent_id)),
      BlockStats(
        BlockId.fromStringUnsafe(row.getString(header_id)),
        row.getLong(timestamp),
        row.getInt(height),
        row.getLong(difficulty),
        row.getInt(block_size),
        row.getLong(block_coins),
        Option(row.getLong(block_mining_time)),
        row.getInt(txs_count),
        row.getInt(txs_size),
        Address.fromStringUnsafe(row.getString(miner_address)),
        row.getLong(miner_reward),
        row.getLong(miner_revenue),
        row.getLong(block_fee),
        row.getLong(block_chain_total_size),
        row.getLong(total_txs_count),
        row.getLong(total_coins_issued),
        row.getLong(total_mining_time),
        row.getLong(total_fees),
        row.getLong(total_miners_reward),
        row.getLong(total_coins_in_txs),
        row.getLong(max_tx_gix),
        row.getLong(max_box_gix),
        row.getBoolean(main_chain)
      )
    )
  }

}
