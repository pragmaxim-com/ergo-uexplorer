package org.ergoplatform.uexplorer.cassandra.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{BoundStatement, DefaultBatchType, PreparedStatement}
import com.typesafe.scalalogging.LazyLogging
import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.cassandra.CassandraBackend
import org.ergoplatform.uexplorer.db.{BestBlockInserted, FullBlock}

import scala.collection.immutable.ArraySeq

trait CassandraTransactionsWriter extends LazyLogging { this: CassandraBackend =>
  import Transactions.*

  def transactionsWriteFlow(parallelism: Int): Flow[BestBlockInserted, BestBlockInserted, NotUsed] =
    storeBatchFlow(
      parallelism,
      batchType = DefaultBatchType.LOGGED,
      buildInsertStatement(columns, node_transactions_table),
      transactionsInsertBinder
    )

  protected[cassandra] def transactionsInsertBinder: (BestBlockInserted, PreparedStatement) => ArraySeq[BoundStatement] = {
    case (BestBlockInserted(_, Some(block)), statement) =>
      block.txs.map { tx =>
        // format: off
        statement
          .bind()
          .setString(header_id,     tx.headerId)
          .setString(tx_id,         tx.id.unwrapped)
          .setInt(inclusion_height, tx.inclusionHeight)
          .setBoolean(coinbase,     tx.isCoinbase)
          .setLong(timestamp,       tx.timestamp)
          .setInt(size,             tx.size)
          .setShort(idx,            tx.index)
          .setLong(global_index,    tx.globalIndex)
          .setBoolean(main_chain,   tx.mainChain)
        // format: on
      }
    case _ =>
      throw new IllegalStateException("Backend must be enabled")

  }

}

object Transactions {
  protected[cassandra] val node_transactions_table = "node_transactions"

  protected[cassandra] val header_id        = "header_id"
  protected[cassandra] val tx_id            = "tx_id"
  protected[cassandra] val inclusion_height = "inclusion_height"
  protected[cassandra] val coinbase         = "coinbase"
  protected[cassandra] val timestamp        = "timestamp"
  protected[cassandra] val size             = "size"
  protected[cassandra] val idx              = "idx"
  protected[cassandra] val global_index     = "global_index"
  protected[cassandra] val main_chain       = "main_chain"

  protected[cassandra] val columns = Seq(
    header_id,
    tx_id,
    inclusion_height,
    coinbase,
    timestamp,
    size,
    idx,
    global_index,
    main_chain
  )
}
