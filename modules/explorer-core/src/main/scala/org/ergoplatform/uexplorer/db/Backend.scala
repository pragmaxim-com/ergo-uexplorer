package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.BlockId
import zio.Task

trait Backend {

  def isEmpty: Task[Boolean]

  def removeBlocks(blockIds: Set[BlockId]): Task[Unit]

  def writeBlock(b: LinkedBlock, condition: Task[Any]): Task[BlockId]

  def close(): Task[Unit]
}
