package org.ergoplatform.uexplorer.indexer.cassandra.entity

import akka.stream.scaladsl.{Sink, Source}
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement, Row, SimpleStatement}
import com.datastax.oss.driver.api.core.data.TupleValue
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.Const
import org.ergoplatform.uexplorer.indexer.cassandra.{CassandraBackend, CassandraPersistenceSupport, EpochPersistenceSupport}
import org.ergoplatform.uexplorer.indexer.progress.ProgressState.CachedBlockInfo
import org.ergoplatform.uexplorer.{db, Address, BlockId, BoxId}

import scala.collection.immutable.{ArraySeq, TreeMap, TreeSet}
import scala.compat.java8.FutureConverters.*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}
import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.indexer.progress.{ProgressState, UtxoState}

import scala.jdk.CollectionConverters.*

trait CassandraEpochReader extends EpochPersistenceSupport with LazyLogging {
  this: CassandraBackend =>

  import CassandraEpochReader._

  private val blockInfoSelectPreparedStatement = cqlSession.prepare(blockInfoSelectStatement)

  def getCachedBlockInfo(
    headerId: BlockId
  ): Future[CachedBlockInfo] =
    cqlSession
      .executeAsync(blockInfoSelectBinder(blockInfoSelectPreparedStatement)(headerId))
      .toScala
      .map(_.one())
      .map(r => blockInfoRowReader(r))

  def getCachedState: Future[ProgressState] = {
    logger.info(s"Loading epoch cache from db ")
    val sortedEpochIndexesF =
      Source
        .fromPublisher(
          cqlSession.executeReactive(
            s"SELECT $epoch_index, $last_header_id FROM ${Const.CassandraKeyspace}.$node_epochs_table;"
          )
        )
        .mapAsync(1) { row =>
          getCachedBlockInfo(BlockId.fromStringUnsafe(row.getString(last_header_id)))
            .map(row.getInt(epoch_index) -> _)
        }
        .runWith(Sink.seq[(Int, CachedBlockInfo)])
        .map(infoByIndex => TreeMap(infoByIndex: _*))
        .andThen { case Success(infoByIndex) =>
          val rangeOpt = infoByIndex.headOption.map(head => s": from ${head._1} to ${infoByIndex.last._1}").getOrElse("")
          logger.info(s"Loaded ${infoByIndex.size} epochs $rangeOpt")
        }

    Source
      .future(sortedEpochIndexesF)
      .mapConcat(identity)
      .mapAsync(1) { case (epochIndex, _) =>
        cqlSession
          .executeAsync(
            s"SELECT $epoch_index, $input_box_ids, $output_box_ids_with_address FROM ${Const.CassandraKeyspace}.$node_epochs_table WHERE $epoch_index = $epochIndex;"
          )
          .toScala
      }
      .map { rs =>
        val r          = rs.one()
        val epochIndex = r.getInt(epoch_index)
        print(s"$epochIndex, ")
        val inputBoxIds = r.getList(input_box_ids, classOf[String]).asScala.map(BoxId(_))
        val outputBoxIdsWithAddress = r.getList(output_box_ids_with_address, classOf[TupleValue]).asScala.map { tuple =>
          BoxId(tuple.getString(0)) -> Address.fromStringUnsafe(tuple.getString(1))
        }
        ArraySeq.from[BoxId](inputBoxIds) -> ArraySeq.from[(BoxId, Address)](outputBoxIdsWithAddress)
      }
      .runFold[UtxoState](UtxoState.empty) { case (s, (inputs, outputs)) => UtxoState.mergeEpoch(s, inputs, outputs) }
      .flatMap { utxoState =>
        sortedEpochIndexesF.map { cachedBlockInfoByEpochIndex =>
          ProgressState.load(cachedBlockInfoByEpochIndex, utxoState)
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

  protected[cassandra] def blockInfoRowReader(row: Row): CachedBlockInfo = {
    import Headers.BlockInfo._
    val blockInfoUdt = row.getUdtValue(Headers.BlockInfo.udtName)
    CachedBlockInfo(
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
