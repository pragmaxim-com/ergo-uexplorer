package org.ergoplatform.uexplorer.db

import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.{Address, Const, ProtocolSettings, Revision}
import org.ergoplatform.{ErgoAddressEncoder, ErgoScriptPredef, Pay2SAddress}
import scorex.util.encode.Base16
import sigmastate.basics.DLogProtocol.ProveDlog
import sigmastate.serialization.{GroupElementSerializer, SigmaSerializer}

import scala.util.Try

object BlockInfoBuilder {

  // CPU greedy (2% of all runtime)
  private def minerRewardAddress(
    apiBlock: ApiFullBlock
  )(protocolSettings: ProtocolSettings): Try[Address] =
    Base16
      .decode(apiBlock.header.minerPk)
      .flatMap { bytes =>
        Try(GroupElementSerializer.parse(SigmaSerializer.startReader(bytes)))
      }
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
      .flatMap(_.outputs)
      .filter(_.ergoTree == Const.FeePropositionScriptHex)
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

  def apply(apiBlock: ApiFullBlock, prevBlock: Option[BlockInfo], parentVersion: Revision)(implicit
    protocolSettings: ProtocolSettings
  ): Try[BlockInfo] =
    minerRewardAddress(apiBlock)(protocolSettings).map { minerAddress =>
      val (reward, fee) = minerRewardAndFee(apiBlock)(protocolSettings)
      val coinBaseValue = reward + fee
      val blockCoins = apiBlock.transactions.transactions
        .flatMap(_.outputs)
        .map(_.value)
        .sum - coinBaseValue
      val miningTime = apiBlock.header.timestamp - prevBlock
        .map(_.timestamp)
        .getOrElse(0L)

      val lastGlobalTxIndex  = prevBlock.map(_.maxTxGix).getOrElse(-1L)
      val lastGlobalBoxIndex = prevBlock.map(_.maxBoxGix).getOrElse(-1L)
      val maxGlobalTxIndex   = lastGlobalTxIndex + apiBlock.transactions.transactions.size
      val maxGlobalBoxIndex = lastGlobalBoxIndex + apiBlock.transactions.transactions.foldLeft(0) { case (sum, tx) =>
        sum + tx.outputs.size
      }

      BlockInfo(
        revision   = parentVersion,
        parentId        = apiBlock.header.parentId,
        timestamp       = apiBlock.header.timestamp,
        height          = apiBlock.header.height,
        blockSize       = apiBlock.size,
        blockCoins      = blockCoins,
        blockMiningTime = prevBlock.map(parent => apiBlock.header.timestamp - parent.timestamp).getOrElse(0),
        txsCount        = apiBlock.transactions.transactions.length,
        txsSize         = apiBlock.transactions.transactions.map(_.size).sum,
        minerAddress    = minerAddress,
        minerReward     = reward,
        minerRevenue    = reward + fee,
        blockFee        = fee,
        blockChainTotalSize = prevBlock
          .map(_.blockChainTotalSize)
          .getOrElse(0L) + apiBlock.size,
        totalTxsCount = apiBlock.transactions.transactions.length.toLong + prevBlock
          .map(_.totalTxsCount)
          .getOrElse(0L),
        totalCoinsIssued = protocolSettings.emission.issuedCoinsAfterHeight(apiBlock.header.height.toLong),
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
