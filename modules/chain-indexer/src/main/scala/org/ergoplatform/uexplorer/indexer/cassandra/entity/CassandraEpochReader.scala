package org.ergoplatform.uexplorer.indexer.cassandra.entity

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.oss.driver.api.core.cql.*
import com.datastax.oss.driver.api.core.data.TupleValue
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.typesafe.scalalogging.LazyLogging
import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.indexer.{MapPimp, MutableMapPimp}
import org.ergoplatform.uexplorer.indexer.cassandra.{CassandraBackend, CassandraPersistenceSupport, EpochPersistenceSupport}
import org.ergoplatform.uexplorer.indexer.chain.ChainState.BufferedBlockInfo
import org.ergoplatform.uexplorer.indexer.chain.{ChainState, Epoch}
import org.ergoplatform.uexplorer.indexer.utxo.{UtxoSnapshot, UtxoState}
import org.ergoplatform.uexplorer.{db, indexer, Address, BlockId, BoxId, Const}

import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.{ArraySeq, TreeMap, TreeSet}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.FutureConverters.*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success}

trait CassandraEpochReader extends EpochPersistenceSupport with LazyLogging {
  this: CassandraBackend =>

  import CassandraEpochReader.*

  private lazy val blockInfoSelectWhereHeader = cqlSession.prepare(blockInfoSelectStatement)

  private def blockInfoByEpochIndex(
    epochIndex: Int,
    headerId: String
  ): Future[(Int, BufferedBlockInfo)] =
    cqlSession
      .executeAsync(blockInfoSelectWhereHeader.bind(headerId))
      .asScala
      .map(_.one())
      .map(r => epochIndex -> blockInfoRowReader(r))

  def loadBlockInfoByEpochIndex: Future[TreeMap[Int, BufferedBlockInfo]] =
    Source
      .fromPublisher(
        cqlSession.executeReactive(
          s"SELECT $epoch_index, $last_header_id FROM ${indexer.Const.CassandraKeyspace}.$node_epoch_last_headers_table;"
        )
      )
      .mapAsync(1)(row => blockInfoByEpochIndex(row.getInt(epoch_index), row.getString(last_header_id)))
      .runWith(Sink.seq[(Int, BufferedBlockInfo)])
      .map(TreeMap(_: _*))
      .andThen { case Success(infoByIndex) =>
        if (infoByIndex.isEmpty)
          logger.info(s"Starting with empty chain ...")
        else
          logger.info(s"${infoByIndex.size} epoch indexes loaded from ${infoByIndex.firstKey} to ${infoByIndex.lastKey}")
      }
}

object CassandraEpochReader extends CassandraPersistenceSupport {

  protected[cassandra] val blockInfoSelectStatement: SimpleStatement =
    QueryBuilder
      .selectFrom(indexer.Const.CassandraKeyspace, Headers.node_headers_table)
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

  protected[cassandra] def blockInfoRowReader(row: Row): BufferedBlockInfo = {
    import Headers.BlockInfo.*
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
