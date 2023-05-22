package org.ergoplatform.uexplorer.cassandra.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.internal.core.`type`.UserDefinedTypeBuilder
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.cassandra.CassandraBackend
import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.cassandra

trait CassandraHeaderWriter extends LazyLogging { this: CassandraBackend =>
  import Headers._

  def headerWriteFlow(parallelism: Int): Flow[Block, Block, NotUsed] =
    storeFlow(
      parallelism,
      buildInsertStatement(columns, node_headers_table),
      headerInsertBinder
    )

  protected[cassandra] def headerInsertBinder: (Block, PreparedStatement) => BoundStatement = { case (block, statement) =>
    val validVersion =
      if (block.header.version.toInt > 255 || block.header.version.toInt < 0) {
        logger.error(s"Version of block ${block.header.id} is out of [8-bit unsigned] range : ${block.header.version}")
        0: Byte
      } else {
        block.header.version
      }

    val partialStatement =
      // format: off
        statement
          .bind()
          .setString(header_id,             block.header.id)
          .setString(parent_id,             block.header.parentId)
          .setByte(version,                 validVersion)
          .setInt(height,                   block.header.height)
          .setLong(n_bits,                  block.header.nBits)
          .setBigDecimal(difficulty,        block.header.difficulty.bigDecimal)
          .setLong(timestamp,               block.header.timestamp)
          .setString(state_root,            block.header.stateRoot)
          .setString(ad_proofs_root,        block.header.adProofsRoot)
          .setString(extensions_digest,     block.extension.digest)
          .setString(extensions_fields,     block.extension.fields.noSpaces)
          .setString(transactions_root,     block.header.transactionsRoot)
          .setString(extension_hash,        block.header.extensionHash)
          .setString(miner_pk,              block.header.minerPk)
          .setString(w,                     block.header.w)
          .setString(n,                     block.header.n)
          .setString(d,                     block.header.d)
          .setString(votes,                 block.header.votes)
          .setBoolean(main_chain,           block.header.mainChain)
          .setUdtValue(BlockInfo.udtName,   BlockInfo.buildUdtValue(block))
        // format: on

    block.adProofOpt.fold(partialStatement) { adProof =>
      partialStatement
        .setString(ad_proofs_bytes, adProof.proofBytes)
        .setString(ad_proofs_digest, adProof.digest)
    }
  }

}

object Headers {
  protected[cassandra] val node_headers_table = "node_headers"

  protected[cassandra] val header_id         = "header_id"
  protected[cassandra] val parent_id         = "parent_id"
  protected[cassandra] val version           = "version"
  protected[cassandra] val height            = "height"
  protected[cassandra] val n_bits            = "n_bits"
  protected[cassandra] val difficulty        = "difficulty"
  protected[cassandra] val timestamp         = "timestamp"
  protected[cassandra] val state_root        = "state_root"
  protected[cassandra] val ad_proofs_root    = "ad_proofs_root"
  protected[cassandra] val ad_proofs_bytes   = "ad_proofs_bytes"
  protected[cassandra] val ad_proofs_digest  = "ad_proofs_digest"
  protected[cassandra] val extensions_digest = "extensions_digest"
  protected[cassandra] val extensions_fields = "extensions_fields"
  protected[cassandra] val transactions_root = "transactions_root"
  protected[cassandra] val extension_hash    = "extension_hash"
  protected[cassandra] val miner_pk          = "miner_pk"
  protected[cassandra] val w                 = "w"
  protected[cassandra] val n                 = "n"
  protected[cassandra] val d                 = "d"
  protected[cassandra] val votes             = "votes"
  protected[cassandra] val main_chain        = "main_chain"

  protected[cassandra] val columns = Seq(
    header_id,
    parent_id,
    version,
    height,
    n_bits,
    difficulty,
    timestamp,
    state_root,
    ad_proofs_root,
    ad_proofs_bytes,
    ad_proofs_digest,
    extensions_digest,
    extensions_fields,
    transactions_root,
    extension_hash,
    miner_pk,
    w,
    n,
    d,
    votes,
    main_chain,
    BlockInfo.udtName
  )

  object BlockInfo {
    val udtName = "block_info"

    val block_size             = "block_size"
    val block_coins            = "block_coins"
    val block_mining_time      = "block_mining_time"
    val txs_count              = "txs_count"
    val txs_size               = "txs_size"
    val miner_address          = "miner_address"
    val miner_reward           = "miner_reward"
    val miner_revenue          = "miner_revenue"
    val block_fee              = "block_fee"
    val block_chain_total_size = "block_chain_total_size"
    val total_txs_count        = "total_txs_count"
    val total_coins_issued     = "total_coins_issued"
    val total_mining_time      = "total_mining_time"
    val total_fees             = "total_fees"
    val total_miners_reward    = "total_miners_reward"
    val total_coins_in_txs     = "total_coins_in_txs"
    val max_tx_gix             = "max_tx_gix"
    val max_box_gix            = "max_box_gix"

    // format: off
    private lazy val udt =
      new UserDefinedTypeBuilder(cassandra.Const.CassandraKeyspace, udtName)
        .withField(block_size,              DataTypes.INT)
        .withField(block_coins,             DataTypes.BIGINT)
        .withField(block_mining_time,       DataTypes.BIGINT)
        .withField(txs_count,               DataTypes.INT)
        .withField(txs_size,                DataTypes.INT)
        .withField(miner_address,           DataTypes.TEXT)
        .withField(miner_reward,            DataTypes.BIGINT)
        .withField(miner_revenue,           DataTypes.BIGINT)
        .withField(block_fee,               DataTypes.BIGINT)
        .withField(block_chain_total_size,  DataTypes.BIGINT)
        .withField(total_txs_count,         DataTypes.BIGINT)
        .withField(total_coins_issued,      DataTypes.BIGINT)
        .withField(total_mining_time,       DataTypes.BIGINT)
        .withField(total_fees,              DataTypes.BIGINT)
        .withField(total_miners_reward,     DataTypes.BIGINT)
        .withField(total_coins_in_txs,      DataTypes.BIGINT)
        .withField(max_tx_gix,              DataTypes.BIGINT)
        .withField(max_box_gix,             DataTypes.BIGINT)
        .build()
    def buildUdtValue(b: Block): UdtValue =
      udt.newValue()
        .setInt(    block_size,             b.info.blockSize)
        .setLong(   block_coins,            b.info.blockCoins)
        .setLong(   block_mining_time,      b.info.blockMiningTime.getOrElse(0L))
        .setInt(    txs_count,              b.info.txsCount)
        .setInt(    txs_size,               b.info.txsSize)
        .setString( miner_address,          b.info.minerAddress)
        .setLong(   miner_reward,           b.info.minerReward)
        .setLong(   miner_revenue,          b.info.minerRevenue)
        .setLong(   block_fee,              b.info.blockFee)
        .setLong(   block_chain_total_size, b.info.blockChainTotalSize)
        .setLong(   total_txs_count,        b.info.totalTxsCount)
        .setLong(   total_coins_issued,     b.info.totalCoinsIssued)
        .setLong(   total_mining_time,      b.info.totalMiningTime)
        .setLong(   total_fees,             b.info.totalFees)
        .setLong(   total_miners_reward,    b.info.totalMinersReward)
        .setLong(   total_coins_in_txs,     b.info.totalCoinsInTxs)
        .setLong(   max_tx_gix,             b.info.maxTxGix)
        .setLong(   max_box_gix,            b.info.maxBoxGix)
    // format: on

  }

}
