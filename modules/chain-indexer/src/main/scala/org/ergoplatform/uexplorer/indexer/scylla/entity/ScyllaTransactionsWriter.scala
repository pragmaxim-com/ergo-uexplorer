package org.ergoplatform.uexplorer.indexer.scylla.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{BoundStatement, DefaultBatchType, PreparedStatement}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.uexplorer.indexer.scylla.ScyllaBackend

trait ScyllaTransactionsWriter extends LazyLogging { this: ScyllaBackend =>
  import Transactions._

  def transactionsWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed] =
    storeBlockBatchFlow(
      parallelism,
      batchType = DefaultBatchType.LOGGED,
      buildInsertStatement(columns, node_transactions_table),
      transactionsInsertBinder
    )

  protected[scylla] def transactionsInsertBinder: (FlatBlock, PreparedStatement) => List[BoundStatement] = {
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
  protected[scylla] val node_transactions_table = "node_transactions"

  protected[scylla] val header_id        = "header_id"
  protected[scylla] val tx_id            = "tx_id"
  protected[scylla] val inclusion_height = "inclusion_height"
  protected[scylla] val coinbase         = "coinbase"
  protected[scylla] val timestamp        = "timestamp"
  protected[scylla] val size             = "size"
  protected[scylla] val idx              = "idx"
  protected[scylla] val global_index     = "global_index"
  protected[scylla] val main_chain       = "main_chain"

  protected[scylla] val columns = Seq(
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
