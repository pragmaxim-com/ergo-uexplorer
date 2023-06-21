package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.Const.Protocol
import org.ergoplatform.uexplorer.Const.Protocol.Emission
import org.ergoplatform.uexplorer.{Address, BlockId, ProtocolSettings, Revision}

final case class BlockInfo(
  revision: Revision,
  parentId: BlockId,
  timestamp: Long,
  height: Int,
  blockSize: Int, // block size (bytes)
  blockCoins: Long, // total amount of nERGs in the block
  blockMiningTime: Long, // block mining time
  txsCount: Int, // number of txs in the block
  txsSize: Int, // total size of all transactions in this block (bytes)
  minerAddress: Address,
  minerReward: Long, // total amount of nERGs miner received from coinbase
  minerRevenue: Long, // total amount of nERGs miner received as a reward (coinbase + fee)
  blockFee: Long, // total amount of transaction fee in the block (nERG)
  blockChainTotalSize: Long, // cumulative blockchain size including this block
  totalTxsCount: Long, // total number of txs in all blocks in the chain
  totalCoinsIssued: Long, // amount of nERGs issued in the block
  totalMiningTime: Long, // mining time of all the blocks in the chain
  totalFees: Long, // total amount of nERGs all miners received as a fee
  totalMinersReward: Long, // total amount of nERGs all miners received as a reward for all time
  totalCoinsInTxs: Long, // total amount of nERGs in all blocks
  maxTxGix: Long, // Global index of the last transaction in the block
  maxBoxGix: Long // Global index of the last output in the last transaction in the block
) {

  def this() = this(
    0,
    Protocol.blockId,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    Emission.address,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0
  ) // kryo needs a no-arg constructor

  def persistable(revision: Revision): BlockInfo = copy(revision = revision)

}

object BlockInfo {
  def apply(ppBlock: BlockWithOutputs, prevBlock: Option[BlockInfo])(implicit
    protocolSettings: ProtocolSettings
  ): BlockInfo = {
    val MinerRewardInfo(reward, fee, minerAddress) = ppBlock.minerRewardInfo
    val coinBaseValue                              = reward + fee
    val blockCoins = ppBlock.b.transactions.transactions
      .flatMap(_.outputs)
      .map(_.value)
      .sum - coinBaseValue
    val miningTime = ppBlock.b.header.timestamp - prevBlock
      .map(_.timestamp)
      .getOrElse(0L)

    val lastGlobalTxIndex  = prevBlock.map(_.maxTxGix).getOrElse(-1L)
    val lastGlobalBoxIndex = prevBlock.map(_.maxBoxGix).getOrElse(-1L)
    val maxGlobalTxIndex   = lastGlobalTxIndex + ppBlock.b.transactions.transactions.size
    val maxGlobalBoxIndex = lastGlobalBoxIndex + ppBlock.b.transactions.transactions.foldLeft(0) { case (sum, tx) =>
      sum + tx.outputs.size
    }

    BlockInfo(
      revision        = 0L, // needs to be updated
      parentId        = ppBlock.b.header.parentId,
      timestamp       = ppBlock.b.header.timestamp,
      height          = ppBlock.b.header.height,
      blockSize       = ppBlock.b.size,
      blockCoins      = blockCoins,
      blockMiningTime = prevBlock.map(parent => ppBlock.b.header.timestamp - parent.timestamp).getOrElse(0),
      txsCount        = ppBlock.b.transactions.transactions.length,
      txsSize         = ppBlock.b.transactions.transactions.map(_.size).sum,
      minerAddress    = minerAddress,
      minerReward     = reward,
      minerRevenue    = reward + fee,
      blockFee        = fee,
      blockChainTotalSize = prevBlock
        .map(_.blockChainTotalSize)
        .getOrElse(0L) + ppBlock.b.size,
      totalTxsCount = ppBlock.b.transactions.transactions.length.toLong + prevBlock
        .map(_.totalTxsCount)
        .getOrElse(0L),
      totalCoinsIssued = protocolSettings.emission.issuedCoinsAfterHeight(ppBlock.b.header.height.toLong),
      totalMiningTime = prevBlock
        .map(_.totalMiningTime)
        .getOrElse(0L) + miningTime,
      totalFees = prevBlock.map(_.totalFees).getOrElse(0L) + fee,
      totalMinersReward = prevBlock
        .map(_.totalMinersReward)
        .getOrElse(0L) + reward,
      totalCoinsInTxs = prevBlock.map(_.totalCoinsInTxs).getOrElse(0L) + blockCoins,
      maxTxGix        = maxGlobalTxIndex,
      maxBoxGix       = maxGlobalBoxIndex
    )
  }

}
