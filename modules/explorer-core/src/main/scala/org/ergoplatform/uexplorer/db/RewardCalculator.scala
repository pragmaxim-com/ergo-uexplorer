package org.ergoplatform.uexplorer.db

import org.ergoplatform.{ErgoAddressEncoder, ErgoScriptPredef, Pay2SAddress}
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ExpandedRegister}
import org.ergoplatform.uexplorer.parser.{ErgoTreeParser, RegistersParser}
import scorex.util.encode.Base16
import sigmastate.basics.DLogProtocol.ProveDlog
import sigmastate.serialization.{GroupElementSerializer, SigmaSerializer}
import eu.timepit.refined.auto.*

import scala.collection.immutable.ArraySeq
import scala.util.Try

case class MinerRewardInfo(reward: MinerReward, fee: MinerFee, address: Address)

object RewardCalculator {

  // CPU greedy (2% of all runtime)
  private def getMinerRewardAddress(
    apiBlock: ApiFullBlock
  )(implicit protocolSettings: ProtocolSettings): Try[Address] =
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

  private def getMinerRewardAndFee(
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

  def apply(block: ApiFullBlock)(implicit protocolSettings: ProtocolSettings): Try[BlockWithReward] =
    for {
      minerRewardAddress <- getMinerRewardAddress(block)
      (reward, fee) = getMinerRewardAndFee(block)
      info          = MinerRewardInfo(reward, fee, minerRewardAddress)
    } yield BlockWithReward(block, info)

}
