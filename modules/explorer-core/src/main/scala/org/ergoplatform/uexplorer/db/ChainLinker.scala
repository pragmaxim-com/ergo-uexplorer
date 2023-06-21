package org.ergoplatform.uexplorer.db

import com.typesafe.scalalogging.LazyLogging
import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.*
import org.ergoplatform.{ErgoAddressEncoder, ErgoScriptPredef, Pay2SAddress}
import scorex.util.encode.Base16
import sigmastate.basics.DLogProtocol.ProveDlog
import sigmastate.serialization.{GroupElementSerializer, SigmaSerializer}
import concurrent.ExecutionContext.Implicits.global
import scala.collection.concurrent
import scala.concurrent.Future
import scala.util.Try

class ChainLinker(getBlock: BlockId => Future[ApiFullBlock], linkedChainTip: concurrent.Map[BlockId, BlockInfo])
  extends LazyLogging {

  def linkChildToAncestors(
    block: BlockWithOutputs,
    acc: List[LinkedBlock]
  )(implicit protocolSettings: ProtocolSettings, enc: ErgoAddressEncoder): Future[List[LinkedBlock]] =
    linkedChainTip.get(block.block.header.parentId).filter(_.height == block.block.header.height - 1) match {
      case someParentInfo @ Some(parentInfo) =>
        val blockInfo = newInfo(block, someParentInfo)
        linkedChainTip.put(block.block.header.id, blockInfo)
        Future.successful(block.toLinkedBlock(blockInfo, someParentInfo) :: acc)
      case _ if block.block.header.height == 1 =>
        val blockInfo = newInfo(block, None)
        linkedChainTip.put(block.block.header.id, blockInfo)
        Future.successful(block.toLinkedBlock(blockInfo, None) :: acc)
      case _ =>
        logger.info(s"Encountered fork at height ${block.block.header.height} and block ${block.block.header.id}")
        for {
          apiBlock     <- getBlock(block.block.header.parentId)
          rewardBlock  <- Future.fromTry(RewardCalculator(apiBlock))
          outputBlock  <- Future.fromTry(OutputParser(rewardBlock))
          linkedBlocks <- linkChildToAncestors(outputBlock, acc)
        } yield linkedBlocks
    }

  def newInfo(ppBlock: BlockWithOutputs, prevBlock: Option[BlockInfo])(implicit
    protocolSettings: ProtocolSettings
  ): BlockInfo = {
    val MinerRewardInfo(reward, fee, minerAddress) = ppBlock.minerRewardInfo
    val coinBaseValue                              = reward + fee
    val blockCoins = ppBlock.block.transactions.transactions
      .flatMap(_.outputs)
      .map(_.value)
      .sum - coinBaseValue
    val miningTime = ppBlock.block.header.timestamp - prevBlock
      .map(_.timestamp)
      .getOrElse(0L)

    val lastGlobalTxIndex  = prevBlock.map(_.maxTxGix).getOrElse(-1L)
    val lastGlobalBoxIndex = prevBlock.map(_.maxBoxGix).getOrElse(-1L)
    val maxGlobalTxIndex   = lastGlobalTxIndex + ppBlock.block.transactions.transactions.size
    val maxGlobalBoxIndex = lastGlobalBoxIndex + ppBlock.block.transactions.transactions.foldLeft(0) { case (sum, tx) =>
      sum + tx.outputs.size
    }

    BlockInfo(
      revision        = 0L, // needs to be updated
      parentId        = ppBlock.block.header.parentId,
      timestamp       = ppBlock.block.header.timestamp,
      height          = ppBlock.block.header.height,
      blockSize       = ppBlock.block.size,
      blockCoins      = blockCoins,
      blockMiningTime = prevBlock.map(parent => ppBlock.block.header.timestamp - parent.timestamp).getOrElse(0),
      txsCount        = ppBlock.block.transactions.transactions.length,
      txsSize         = ppBlock.block.transactions.transactions.map(_.size).sum,
      minerAddress    = minerAddress,
      minerReward     = reward,
      minerRevenue    = reward + fee,
      blockFee        = fee,
      blockChainTotalSize = prevBlock
        .map(_.blockChainTotalSize)
        .getOrElse(0L) + ppBlock.block.size,
      totalTxsCount = ppBlock.block.transactions.transactions.length.toLong + prevBlock
        .map(_.totalTxsCount)
        .getOrElse(0L),
      totalCoinsIssued = protocolSettings.emission.issuedCoinsAfterHeight(ppBlock.block.header.height.toLong),
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
