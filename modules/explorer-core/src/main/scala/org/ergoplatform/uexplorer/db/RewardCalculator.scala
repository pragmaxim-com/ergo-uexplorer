package org.ergoplatform.uexplorer.db

import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ExpandedRegister}
import org.ergoplatform.uexplorer.parser.{ErgoTreeParser, RegistersParser}
import org.ergoplatform.{ErgoAddressEncoder, ErgoScriptPredef, Pay2SAddress}
import scorex.util.encode.Base16
import sigmastate.basics.DLogProtocol.ProveDlog
import sigmastate.serialization.{GroupElementSerializer, SigmaSerializer}
import zio.{Task, ZIO}

import scala.collection.immutable.ArraySeq
import scala.util.Try

case class MinerRewardInfo(reward: MinerReward, fee: MinerFee, address: Address)

object RewardCalculator {

  // CPU greedy (2% of all runtime)
  private def getMinerRewardAddress(
    apiBlock: ApiFullBlock
  )(implicit ps: CoreConf): Task[Address] =
    ZIO
      .fromTry(Base16.decode(apiBlock.header.minerPk))
      .map { bytes =>
        val ecPointType = GroupElementSerializer.parse(SigmaSerializer.startReader(bytes))
        val minerPk     = ProveDlog(ecPointType)
        val rewardScript =
          ErgoScriptPredef.rewardOutputScript(
            ps.monetary.minerRewardDelay,
            minerPk
          )
        val addressStr = Pay2SAddress(rewardScript)(ps.addressEncoder).toString
        Address.fromStringUnsafe(addressStr)
      }

  private def getMinerRewardAndFee(apiBlock: ApiFullBlock)(implicit ps: CoreConf): (Long, Long) = {
    val emission = ps.emission.emissionAtHeight(apiBlock.header.height.toLong)
    val reward   = math.min(Const.TeamTreasuryThreshold, emission)
    val eip27Reward =
      if (reward >= Const.Eip27UpperPoint) reward - Const.Eip27DefaultReEmission
      else if (Const.Eip27LowerPoint < reward) reward - (reward - Const.Eip27ResidualEmission)
      else reward
    val fee = apiBlock.transactions.transactions
      .flatMap(_.outputs)
      .filter(_.ergoTree == Const.Protocol.FeeContract.ergoTreeHex)
      .map(_.value)
      .sum
    ps.networkPrefix.value.toByte match {
      case ErgoAddressEncoder.MainnetNetworkPrefix if apiBlock.header.height >= Const.MainnetEip27ActivationHeight =>
        (eip27Reward, fee)
      case ErgoAddressEncoder.TestnetNetworkPrefix if apiBlock.header.height >= Const.TestnetEip27ActivationHeight =>
        (eip27Reward, fee)
      case _ =>
        (reward, fee)
    }
  }

  def apply(block: ApiFullBlock)(implicit ps: CoreConf): Task[BlockWithReward] =
    for {
      minerRewardAddress <- getMinerRewardAddress(block)
      (reward, fee) = getMinerRewardAndFee(block)
      info          = MinerRewardInfo(reward, fee, minerRewardAddress)
    } yield BlockWithReward(block, info)

}
