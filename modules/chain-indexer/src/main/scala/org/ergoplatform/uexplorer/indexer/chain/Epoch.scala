package org.ergoplatform.uexplorer.indexer.chain

import org.ergoplatform.uexplorer.{indexer, Address, BlockId, BoxId, Const}
import org.ergoplatform.uexplorer.indexer.UnexpectedStateError

import scala.collection.immutable.ArraySeq
import scala.collection.mutable

case class Epoch(
  index: Int,
  blockIds: Vector[BlockId]
)

object Epoch {

  def epochIndexForHeight(height: Int): Int = {
    if (height < 1) throw new UnexpectedStateError("Height must start from 1 as genesis block is not part of an epoch")
    (height - 1) / Const.EpochLength
  }

  def heightRangeForEpochIndex(index: Int): Seq[Int] = {
    if (index < 0) throw new UnexpectedStateError("Negative epoch index is illegal")
    val epochStartHeight = index * Const.EpochLength + 1
    val epochEndHeight   = epochStartHeight + 1023
    epochStartHeight to epochEndHeight
  }

  def heightAtFlushPoint(height: Int): Boolean =
    Epoch.epochIndexForHeight(height) > 0 && height % Const.EpochLength == indexer.Const.FlushHeight

}
