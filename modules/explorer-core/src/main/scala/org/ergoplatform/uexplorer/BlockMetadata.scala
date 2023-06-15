package org.ergoplatform.uexplorer

import org.ergoplatform.uexplorer.db.{BlockInfo, FullBlock, LightBlock}

case class BlockMetadata(
  parentVersion: Long,
  parentId: BlockId,
  timestamp: Long,
  height: Int,
  info: BlockInfo
) {
  def this() =
    this(
      0,
      BlockMetadata.genesisBlock,
      0,
      0,
      new BlockInfo()
    ) // kryo needs a no-arg constructor
}

object BlockMetadata {
  private val genesisBlock = BlockId.fromStringUnsafe("0000000000000000000000000000000000000000000000000000000000000000")
  def fromBlock(b: LightBlock, version: Long): BlockMetadata =
    BlockMetadata(version, b.parentId, b.timestamp, b.height, b.info)
}
