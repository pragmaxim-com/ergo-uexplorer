package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.Err.ProcessingErr
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.{Address, BlockId, Const, ProtocolSettings}
import org.ergoplatform.{ErgoAddressEncoder, ErgoScriptPredef, Pay2SAddress}
import scorex.util.encode.Base16
import sigmastate.basics.DLogProtocol.ProveDlog
import sigmastate.serialization.{GroupElementSerializer, SigmaSerializer}

import scala.util.{Failure, Success, Try}

/** Represents `blocks_info` table.
  * Containing main fields from protocol header and full-block stats.
  */
final case class BlockStats(
  headerId: BlockId,
  timestamp: Long,
  height: Int,
  difficulty: BigInt,
  blockSize: Int, // block size (bytes)
  blockCoins: Long, // total amount of nERGs in the block
  blockMiningTime: Option[Long], // block mining time
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
  maxBoxGix: Long, // Global index of the last output in the last transaction in the block
  mainChain: Boolean
)

object BlockStats {

  private def minerRewardAddress(
    apiBlock: ApiFullBlock
  )(protocolSettings: ProtocolSettings): Try[Address] =
    Base16
      .decode(apiBlock.header.minerPk.unwrapped)
      .flatMap { bytes =>
        Try(GroupElementSerializer.parse(SigmaSerializer.startReader(bytes)))
      }
      .transform(Success(_), ex => Failure(ProcessingErr.EcPointDecodingFailed(ex.getMessage)))
      .flatMap { x =>
        val minerPk = ProveDlog(x)
        val rewardScript =
          ErgoScriptPredef.rewardOutputScript(
            protocolSettings.monetary.minerRewardDelay,
            minerPk
          )
        val addressStr =
          Pay2SAddress(rewardScript)(protocolSettings.addressEncoder).toString
        Try(Address.fromStringUnsafe(addressStr))
      }

  private def minerRewardAndFee(
    apiBlock: ApiFullBlock
  )(implicit protocolSettings: ProtocolSettings): (Long, Long) = {
    val emission = protocolSettings.emission.emissionAtHeight(apiBlock.header.height.toLong)
    val reward   = math.min(Const.TeamTreasuryThreshold, emission)
    val eip27Reward =
      if (reward >= Const.Eip27UpperPoint) reward - Const.Eip27DefaultReEmission
      else if (Const.Eip27LowerPoint < reward) reward - (reward - Const.Eip27ResidualEmission)
      else reward
    val fee = apiBlock.transactions.transactions
      .flatMap(_.outputs.toList)
      .filter(_.ergoTree.unwrapped == Const.FeePropositionScriptHex)
      .map(_.value)
      .sum
    protocolSettings.networkPrefix.value.toByte match {
      case ErgoAddressEncoder.MainnetNetworkPrefix if apiBlock.header.height >= Const.MainnetEip27ActivationHeight =>
        (eip27Reward, fee)
      case ErgoAddressEncoder.TestnetNetworkPrefix if apiBlock.header.height >= Const.TestnetEip27ActivationHeight =>
        (eip27Reward, fee)
      case _ =>
        (reward, fee)
    }
  }

  def apply(apiBlock: ApiFullBlock, prevBlockInfo: Option[BlockStats])(implicit
    protocolSettings: ProtocolSettings
  ): Try[BlockStats] =
    minerRewardAddress(apiBlock)(protocolSettings).map { minerAddress =>
      val (reward, fee) = minerRewardAndFee(apiBlock)(protocolSettings)
      val coinBaseValue = reward + fee
      val blockCoins = apiBlock.transactions.transactions
        .flatMap(_.outputs.toList)
        .map(_.value)
        .sum - coinBaseValue
      val miningTime = apiBlock.header.timestamp - prevBlockInfo
        .map(_.timestamp)
        .getOrElse(0L)

      val lastGlobalTxIndex  = prevBlockInfo.map(_.maxTxGix).getOrElse(-1L)
      val lastGlobalBoxIndex = prevBlockInfo.map(_.maxBoxGix).getOrElse(-1L)
      val maxGlobalTxIndex   = lastGlobalTxIndex + apiBlock.transactions.transactions.size
      val maxGlobalBoxIndex  = lastGlobalBoxIndex + apiBlock.transactions.transactions.flatMap(_.outputs.toList).size

      BlockStats(
        headerId        = apiBlock.header.id,
        timestamp       = apiBlock.header.timestamp,
        height          = apiBlock.header.height,
        difficulty      = apiBlock.header.difficulty.value.toLong,
        blockSize       = apiBlock.size,
        blockCoins      = blockCoins,
        blockMiningTime = prevBlockInfo.map(parent => apiBlock.header.timestamp - parent.timestamp),
        txsCount        = apiBlock.transactions.transactions.length,
        txsSize         = apiBlock.transactions.transactions.map(_.size).sum,
        minerAddress    = minerAddress,
        minerReward     = reward,
        minerRevenue    = reward + fee,
        blockFee        = fee,
        blockChainTotalSize = prevBlockInfo
          .map(_.blockChainTotalSize)
          .getOrElse(0L) + apiBlock.size,
        totalTxsCount = apiBlock.transactions.transactions.length.toLong + prevBlockInfo
          .map(_.totalTxsCount)
          .getOrElse(0L),
        totalCoinsIssued = protocolSettings.emission.issuedCoinsAfterHeight(apiBlock.header.height.toLong),
        totalMiningTime = prevBlockInfo
          .map(_.totalMiningTime)
          .getOrElse(0L) + miningTime,
        totalFees = prevBlockInfo.map(_.totalFees).getOrElse(0L) + fee,
        totalMinersReward = prevBlockInfo
          .map(_.totalMinersReward)
          .getOrElse(0L) + reward,
        totalCoinsInTxs = prevBlockInfo.map(_.totalCoinsInTxs).getOrElse(0L) + blockCoins,
        maxTxGix        = maxGlobalTxIndex,
        maxBoxGix       = maxGlobalBoxIndex,
        mainChain       = false
      )
    }
}
