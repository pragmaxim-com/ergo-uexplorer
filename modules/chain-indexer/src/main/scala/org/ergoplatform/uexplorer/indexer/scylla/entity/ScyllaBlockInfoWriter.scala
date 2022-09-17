package org.ergoplatform.uexplorer.indexer.scylla.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.uexplorer.indexer.scylla.ScyllaBackend

trait ScyllaBlockInfoWriter {
  this: ScyllaBackend =>

  import BlocksInfo._

  def blockInfoWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed] =
    storeBlockFlow(
      parallelism,
      buildInsertStatement(columns, block_info_table),
      blockInfoInsertBinder
    )

  protected[scylla] def blockInfoInsertBinder: (FlatBlock, PreparedStatement) => BoundStatement = { case (b, statement) =>
    // format: off
      statement
        .bind()
        .setString(header_id,             b.header.id.value.unwrapped)
        .setString(parent_id,             b.header.parentId.value.unwrapped)
        .setLong(timestamp,               b.header.timestamp)
        .setInt(height,                   b.header.height)
        .setLong(difficulty,              b.header.difficulty.toLong)
        .setInt(block_size,               b.info.blockSize)
        .setLong(block_coins,             b.info.blockCoins)
        .setLong(block_mining_time,       b.info.blockMiningTime.getOrElse(0L))
        .setInt(txs_count,                b.info.txsCount)
        .setInt(txs_size,                 b.info.txsSize)
        .setString(miner_address,         b.info.minerAddress.unwrapped)
        .setLong(miner_reward,            b.info.minerReward)
        .setLong(miner_revenue,           b.info.minerRevenue)
        .setLong(block_fee,               b.info.blockFee)
        .setLong(block_chain_total_size,  b.info.blockChainTotalSize)
        .setLong(total_txs_count,         b.info.totalTxsCount)
        .setLong(total_coins_issued,      b.info.totalCoinsIssued)
        .setLong(total_mining_time,       b.info.totalMiningTime)
        .setLong(total_fees,              b.info.totalFees)
        .setLong(total_miners_reward,     b.info.totalMinersReward)
        .setLong(total_coins_in_txs,      b.info.totalCoinsInTxs)
        .setLong(max_tx_gix,              b.info.maxTxGix)
        .setLong(max_box_gix,             b.info.maxBoxGix)
        .setBoolean(main_chain,           b.info.mainChain)
      // format: on
  }
}

object BlocksInfo {
  protected[scylla] val block_info_table = "blocks_info"

  protected[scylla] val header_id              = "header_id"
  protected[scylla] val parent_id              = "parent_id"
  protected[scylla] val timestamp              = "timestamp"
  protected[scylla] val height                 = "height"
  protected[scylla] val difficulty             = "difficulty"
  protected[scylla] val block_size             = "block_size"
  protected[scylla] val block_coins            = "block_coins"
  protected[scylla] val block_mining_time      = "block_mining_time"
  protected[scylla] val txs_count              = "txs_count"
  protected[scylla] val txs_size               = "txs_size"
  protected[scylla] val miner_address          = "miner_address"
  protected[scylla] val miner_reward           = "miner_reward"
  protected[scylla] val miner_revenue          = "miner_revenue"
  protected[scylla] val block_fee              = "block_fee"
  protected[scylla] val block_chain_total_size = "block_chain_total_size"
  protected[scylla] val total_txs_count        = "total_txs_count"
  protected[scylla] val total_coins_issued     = "total_coins_issued"
  protected[scylla] val total_mining_time      = "total_mining_time"
  protected[scylla] val total_fees             = "total_fees"
  protected[scylla] val total_miners_reward    = "total_miners_reward"
  protected[scylla] val total_coins_in_txs     = "total_coins_in_txs"
  protected[scylla] val max_tx_gix             = "max_tx_gix"
  protected[scylla] val max_box_gix            = "max_box_gix"
  protected[scylla] val main_chain             = "main_chain"

  lazy val columns: Seq[String] = Seq(
    header_id,
    parent_id,
    timestamp,
    height,
    difficulty,
    block_size,
    block_coins,
    block_mining_time,
    txs_count,
    txs_size,
    miner_address,
    miner_reward,
    miner_revenue,
    block_fee,
    block_chain_total_size,
    total_txs_count,
    total_coins_issued,
    total_mining_time,
    total_fees,
    total_miners_reward,
    total_coins_in_txs,
    max_tx_gix,
    max_box_gix,
    main_chain
  )
}
