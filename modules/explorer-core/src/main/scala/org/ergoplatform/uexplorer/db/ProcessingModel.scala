package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.node.*

import scala.collection.immutable.ArraySeq
import org.ergoplatform.uexplorer.*

case class OutputRecord(
  txId: TxId,
  boxId: BoxId,
  ergoTree: HexString,
  scriptTemplateHash: ErgoTreeTemplateHash,
  address: Address,
  value: Value,
  additionalRegisters: Map[RegisterId, ExpandedRegister]
)

case class BlockWithOutputs(
  block: ApiFullBlock,
  minerRewardInfo: MinerRewardInfo,
  outputRecords: ArraySeq[OutputRecord]
) {
  def toLinkedBlock(blockInfo: BlockInfo, parentInfoOpt: Option[BlockInfo]) =
    LinkedBlock(block, minerRewardInfo, outputRecords, blockInfo, parentInfoOpt)
}

case class InputRecord(
  txId: TxId,
  boxId: BoxId,
  address: Address,
  value: Value
)

case class BlockWithInputs(
  block: ApiFullBlock,
  minerRewardInfo: MinerRewardInfo,
  inputRecords: ArraySeq[InputRecord],
  outputRecords: ArraySeq[OutputRecord],
  blockInfo: BlockInfo,
  parentInfoOpt: Option[BlockInfo]
)

case class LinkedBlock(
  block: ApiFullBlock,
  minerRewardInfo: MinerRewardInfo,
  outputRecords: ArraySeq[OutputRecord],
  blockInfo: BlockInfo,
  parentInfoOpt: Option[BlockInfo]
) {
  def toBlockWithInputs(inputRecords: ArraySeq[InputRecord]) =
    BlockWithInputs(block, minerRewardInfo, inputRecords, outputRecords, blockInfo, parentInfoOpt)
}

case class MinerRewardInfo(reward: MinerReward, fee: MinerFee, address: Address)

case class BlockWithReward(
  block: ApiFullBlock,
  minerRewardInfo: MinerRewardInfo
) {
  def toBlockWithOutput(outputRecords: ArraySeq[OutputRecord]) =
    BlockWithOutputs(block, minerRewardInfo, outputRecords)
}
