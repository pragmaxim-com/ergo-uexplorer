package org.ergoplatform.uexplorer.indexer.cassandra.entity

import akka.stream.scaladsl.{Sink, Source}
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement, Row, SimpleStatement}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.Const
import org.ergoplatform.uexplorer.indexer.cassandra.{CassandraBackend, CassandraPersistenceSupport, EpochPersistenceSupport}
import org.ergoplatform.uexplorer.indexer.progress.ProgressState.CachedBlock
import org.ergoplatform.uexplorer.{db, Address, BlockId}

import scala.collection.immutable.TreeMap
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

trait CassandraEpochReader extends EpochPersistenceSupport with LazyLogging {
  this: CassandraBackend =>

  import CassandraEpochReader._

  private val blockInfoSelectPreparedStatement = cqlSession.prepare(blockInfoSelectStatement)

  def getBlockInfo(headerId: BlockId): Future[CachedBlock] =
    cqlSession
      .executeAsync(blockInfoSelectBinder(blockInfoSelectPreparedStatement)(headerId))
      .toScala
      .map(_.one())
      .map(blockInfoRowReader)

  def getLastBlockInfoByEpochIndex: Future[TreeMap[Int, CachedBlock]] = {
    logger.debug(s"Getting existing epoch indexes from db ")
    Source
      .fromPublisher(
        cqlSession.executeReactive(
          s"SELECT $last_header_id, $epoch_index FROM ${Const.CassandraKeyspace}.$node_epochs_table;"
        )
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
    preparedStatement.bind().setString(Headers.header_id, headerId.value.unwrapped)

  protected[cassandra] def blockInfoRowReader(row: Row): CachedBlock = {
    import Headers.BlockInfo._
    val blockInfoUdt = row.getUdtValue(Headers.BlockInfo.udtName)
    CachedBlock(
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
