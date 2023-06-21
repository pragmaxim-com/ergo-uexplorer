package org.ergoplatform.uexplorer.db

import com.typesafe.scalalogging.LazyLogging
import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ApiHeader}
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.db.ChainTip.FifoLinkedHashMap
import org.ergoplatform.{ErgoAddressEncoder, ErgoScriptPredef, Pay2SAddress}
import scorex.util.encode.Base16
import sigmastate.basics.DLogProtocol.ProveDlog
import sigmastate.serialization.{GroupElementSerializer, SigmaSerializer}

import java.util
import concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable
import scala.collection.immutable.TreeSet
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.jdk.CollectionConverters.*

class ChainTip(byBlockId: FifoLinkedHashMap[BlockId, BlockInfo]) {
  def toMap: Map[BlockId, BlockInfo] = byBlockId.asScala.toMap
  def getParent(block: ApiFullBlock): Option[BlockInfo] =
    Option(byBlockId.get(block.header.parentId)).filter(_.height == block.header.height - 1)
  def putOnlyNew(blockId: BlockId, info: BlockInfo): Try[BlockInfo] =
    Option(byBlockId.put(blockId, info)).fold(Success(info)) { oldVal =>
      Failure(
        new AssertionError(
          s"Trying to cache blockId $blockId at height ${info.height} but there already was $oldVal"
        )
      )
    }
}
object ChainTip {
  def apply(chainTip: IterableOnce[(BlockId, BlockInfo)], maxSize: Int = 100): ChainTip = {
    val newFifoMap = new FifoLinkedHashMap[BlockId, BlockInfo](maxSize)
    newFifoMap.putAll(chainTip.iterator.toMap.asJava)
    new ChainTip(newFifoMap)
  }

  class FifoLinkedHashMap[K, V](maxSize: Int = 100) extends util.LinkedHashMap[K, V] {
    override def removeEldestEntry(eldest: java.util.Map.Entry[K, V]): Boolean = this.size > maxSize
  }
}

class ChainLinker(getBlock: BlockId => Future[ApiFullBlock], chainTip: ChainTip) extends LazyLogging {

  def linkChildToAncestors(acc: List[LinkedBlock] = List.empty)(
    block: BlockWithOutputs
  )(implicit ps: ProtocolSettings): Future[List[LinkedBlock]] =
    chainTip.getParent(block.block) match {
      case parentInfoOpt if parentInfoOpt.isDefined || block.block.header.height == 1 =>
        Future.fromTry(
          chainTip.putOnlyNew(block.block.header.id, newInfo(block, parentInfoOpt)).map { newBlockInfo =>
            block.toLinkedBlock(newBlockInfo, parentInfoOpt) :: acc
          }
        )
      case _ =>
        logger.info(s"Encountered fork at height ${block.block.header.height} and block ${block.block.header.id}")
        for {
          apiBlock     <- getBlock(block.block.header.parentId)
          rewardBlock  <- Future.fromTry(RewardCalculator(apiBlock))
          outputBlock  <- Future.fromTry(OutputParser(rewardBlock)(ps.addressEncoder))
          linkedBlocks <- linkChildToAncestors(acc)(outputBlock)
        } yield linkedBlocks
    }

  private def newInfo(ppBlock: BlockWithOutputs, prevBlock: Option[BlockInfo])(implicit
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
