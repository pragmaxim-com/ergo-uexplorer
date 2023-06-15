package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.db.{BlockInfo, FullBlock, LightBlock}
import org.ergoplatform.uexplorer.{db, BlockId, BoxesByTx}

final case class LightBlock(
  headerId: BlockId,
  parentId: BlockId,
  timestamp: Long,
  height: Int,
  boxesByTx: BoxesByTx,
  info: BlockInfo
) {
  def toVersionedBlock(version: Long) = VersionedBlock(version, parentId, timestamp, height, info)
}

case class VersionedBlock(
  parentVersion: Long,
  parentId: BlockId,
  timestamp: Long,
  height: Int,
  info: BlockInfo
) {
  def this() =
    this(
      0,
      VersionedBlock.genesisBlock,
      0,
      0,
      new BlockInfo()
    ) // kryo needs a no-arg constructor
}

object VersionedBlock {
  private val genesisBlock = BlockId.fromStringUnsafe("0000000000000000000000000000000000000000000000000000000000000000")
}
