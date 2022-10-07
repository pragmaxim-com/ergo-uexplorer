package org.ergoplatform.uexplorer.indexer.cassandra.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{BoundStatement, DefaultBatchType, PreparedStatement}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.FlatBlock
import org.ergoplatform.uexplorer.indexer.cassandra.CassandraBackend

trait CassandraTransactionsWriter extends LazyLogging { this: CassandraBackend =>
  import Transactions._

  def transactionsWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed] =
    storeBlockBatchFlow(
      parallelism,
      batchType = DefaultBatchType.LOGGED,
      buildInsertStatement(columns, node_transactions_table),
      transactionsInsertBinder
    )

  protected[cassandra] def transactionsInsertBinder: (FlatBlock, PreparedStatement) => List[BoundStatement] = {
    case (block, statement) =>
      block.txs.map { tx =>
        // format: off
        statement
          .bind()
          .setString(header_id,     tx.headerId.value.unwrapped)
          .setString(tx_id,         tx.id.value)
          .setInt(inclusion_height, tx.inclusionHeight)
          .setBoolean(coinbase,     tx.isCoinbase)
          .setLong(timestamp,       tx.timestamp)
          .setInt(size,             tx.size)
          .setInt(idx,              tx.index)
          .setLong(global_index,    tx.globalIndex)
          .setBoolean(main_chain,   tx.mainChain)
        // format: on
      }
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
