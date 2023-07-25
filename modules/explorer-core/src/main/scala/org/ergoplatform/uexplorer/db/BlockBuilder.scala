package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.Const.Protocol
import org.ergoplatform.uexplorer.Const.Protocol.Emission
import org.ergoplatform.uexplorer.{Address, BlockId, CoreConf, ErgoTreeHex, Revision}

object BlockBuilder:
  def apply(ppBlock: BlockWithOutputs, prevBlock: Option[Block])(implicit
    coreConf: CoreConf
  ): Block = {
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

    Block(
      blockId         = ppBlock.b.header.id,
      parentId        = ppBlock.b.header.parentId,
      revision        = 0L, // needs to be updated
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
      totalCoinsIssued = coreConf.emission.issuedCoinsAfterHeight(ppBlock.b.header.height.toLong),
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
