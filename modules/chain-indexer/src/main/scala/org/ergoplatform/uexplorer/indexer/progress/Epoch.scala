package org.ergoplatform.uexplorer.indexer.progress

import org.ergoplatform.explorer.BlockId
import org.ergoplatform.uexplorer.indexer.Const

case class Epoch(index: Int, blockIds: Vector[BlockId])

object Epoch {

  def epochIndexForHeight(height: Int): Int = (height - 1) / Const.EpochLength

  def heightRangeForEpochIndex(index: Int): Seq[Int] = {
    val epochStartHeight = index * Const.EpochLength + 1
    val epochEndHeight   = epochStartHeight + 1023
    epochStartHeight to epochEndHeight
  }

}
