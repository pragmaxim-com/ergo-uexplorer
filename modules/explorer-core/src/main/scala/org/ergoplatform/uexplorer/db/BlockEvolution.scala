package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.node.*

import scala.collection.immutable.ArraySeq

case class BlockWithReward(
  b: ApiFullBlock,
  minerRewardInfo: MinerRewardInfo
) {
  def toBlockWithOutput(outputRecords: OutputRecords) =
    BlockWithOutputs(b, minerRewardInfo, outputRecords)
}

case class BlockWithOutputs(
  b: ApiFullBlock,
  minerRewardInfo: MinerRewardInfo,
  outputRecords: OutputRecords
) {
  def toLinkedBlock(block: Block, parentBlockOpt: Option[Block]) =
    LinkedBlock(b, minerRewardInfo, outputRecords, block, parentBlockOpt)
}

case class LinkedBlock(
  b: ApiFullBlock,
  minerRewardInfo: MinerRewardInfo,
  outputRecords: OutputRecords,
  block: Block,
  parentBlockOpt: Option[Block]
)
