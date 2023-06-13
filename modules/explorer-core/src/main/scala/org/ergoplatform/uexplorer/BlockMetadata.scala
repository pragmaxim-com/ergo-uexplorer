package org.ergoplatform.uexplorer

import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.db.BlockInfo

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
  def fromBlock(b: Block, version: Long): BlockMetadata =
    BlockMetadata(version, b.header.parentId, b.header.timestamp, b.header.height, b.info)
}
