package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.node.*

import scala.collection.immutable.ArraySeq
import org.ergoplatform.uexplorer.*

case class BlockWithReward(
  b: ApiFullBlock,
  minerRewardInfo: MinerRewardInfo
) {
  def toBlockWithOutput(outputRecords: ArraySeq[OutputRecord]) =
    BlockWithOutputs(b, minerRewardInfo, outputRecords)
}

case class BlockWithOutputs(
  b: ApiFullBlock,
  minerRewardInfo: MinerRewardInfo,
  outputRecords: ArraySeq[OutputRecord]
) {
  def toLinkedBlock(blockInfo: BlockInfo, parentInfoOpt: Option[BlockInfo]) =
    LinkedBlock(b, minerRewardInfo, outputRecords, blockInfo, parentInfoOpt)
}

case class LinkedBlock(
  b: ApiFullBlock,
  minerRewardInfo: MinerRewardInfo,
  outputRecords: ArraySeq[OutputRecord],
  info: BlockInfo,
  parentInfoOpt: Option[BlockInfo]
) {
  def toBlockWithInputs(inputRecords: ArraySeq[InputRecord]) =
    BlockWithInputs(b, minerRewardInfo, inputRecords, outputRecords, info, parentInfoOpt)
}

case class BlockWithInputs(
  b: ApiFullBlock,
  minerRewardInfo: MinerRewardInfo,
  inputRecords: ArraySeq[InputRecord],
  outputRecords: ArraySeq[OutputRecord],
  info: BlockInfo,
  parentInfoOpt: Option[BlockInfo]
)
