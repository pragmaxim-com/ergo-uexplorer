package org.ergoplatform.uexplorer.indexer.scylla

import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement, Row, SimpleStatement}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.{bindMarker, insertInto}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.{Address, BlockId}
import org.ergoplatform.explorer.db.models.BlockStats
import org.ergoplatform.uexplorer.indexer.Const
import org.ergoplatform.uexplorer.indexer.api.BlockBuilder.BlockInfo
import org.ergoplatform.uexplorer.indexer.api.EpochService
import org.ergoplatform.uexplorer.indexer.progress.Epoch
import org.ergoplatform.uexplorer.indexer.scylla.entity.BlocksInfo

import scala.collection.immutable.TreeMap
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class ScyllaEpochService(implicit session: CqlSession, s: ActorSystem[Nothing]) extends EpochService with LazyLogging {
  import ScyllaEpochService._
  private val blockInfoSelectPreparedStatement = session.prepare(blockInfoSelectStatement)

  def getBlockInfo(headerId: BlockId): Future[BlockInfo] =
    session
      .executeAsync(blockInfoSelectBinder(blockInfoSelectPreparedStatement)(headerId))
      .toScala
      .map(_.one())
      .map(blockInfoRowReader)

  def getLastBlockInfoByEpochIndex: Future[TreeMap[Int, BlockInfo]] = {
    logger.debug(s"Getting existing epoch indexes from db ")
    Source
      .fromPublisher(
        session.executeReactive(s"SELECT $last_header_id, $epoch_index FROM ${Const.ScyllaKeyspace}.$node_epochs_table;")
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

  override def persistEpoch(epoch: Epoch): Future[Epoch] =
    session
      .prepareAsync(epochInsertStatement)
      .toScala
      .map(epochInsertBinder(epoch))
      .flatMap { stmnt =>
        session
          .executeAsync(stmnt)
          .toScala
          .map(_ => epoch)
      }
}

object ScyllaEpochService {

  protected[scylla] val node_epochs_table = "node_epochs"

  protected[scylla] val epoch_index    = "epoch_index"
  protected[scylla] val last_header_id = "last_header_id"

  protected[scylla] def epochInsertBinder(epoch: Epoch)(stmt: PreparedStatement): BoundStatement =
    stmt
      .bind()
      .setInt(epoch_index, epoch.index)
      .setString(last_header_id, epoch.blockIds.last.value.unwrapped)

  protected[scylla] val epochInsertStatement: SimpleStatement =
    insertInto(Const.ScyllaKeyspace, node_epochs_table)
      .value(epoch_index, bindMarker(epoch_index))
      .value(last_header_id, bindMarker(last_header_id))
      .build()
      .setIdempotent(true)

  protected[scylla] val blockInfoSelectStatement: SimpleStatement = {
    import BlocksInfo._
    QueryBuilder
      .selectFrom(Const.ScyllaKeyspace, block_info_table)
      .columns(columns: _*)
      .whereColumn(header_id)
      .isEqualTo(QueryBuilder.bindMarker(header_id))
      .build()
  }

  protected[scylla] def blockInfoSelectBinder(preparedStatement: PreparedStatement)(headerId: BlockId): BoundStatement =
    preparedStatement.bind().setString(BlocksInfo.header_id, headerId.value.unwrapped)

  protected[scylla] def blockInfoRowReader(row: Row): BlockInfo = {
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
