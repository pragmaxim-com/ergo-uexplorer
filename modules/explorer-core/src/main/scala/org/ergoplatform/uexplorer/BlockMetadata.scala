package org.ergoplatform.uexplorer

import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.db.BlockInfo

case class BlockMetadata(headerId: BlockId, parentId: BlockId, timestamp: Long, height: Int, info: BlockInfo)

object BlockMetadata {

  def fromBlock(b: Block): BlockMetadata =
    BlockMetadata(b.header.id, b.header.parentId, b.header.timestamp, b.header.height, b.info)
}
