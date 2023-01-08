package org.ergoplatform.uexplorer.indexer.chain

import org.ergoplatform.uexplorer.{indexer, Address, BlockId, BoxId, Const, EpochIndex, Height}
import org.ergoplatform.uexplorer.indexer.UnexpectedStateError
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState

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

  def heightRangeForEpochIndex(index: EpochIndex): Seq[Height] = {
    if (index < 0) throw new UnexpectedStateError("Negative epoch index is illegal")
    val epochStartHeight = index * Const.EpochLength + 1
    val epochEndHeight   = epochStartHeight + 1023
    epochStartHeight to epochEndHeight
  }

  def heightAtFlushPoint(height: Int): Boolean =
    Epoch.epochIndexForHeight(height) > 0 && height % Const.EpochLength == indexer.Const.FlushHeight

}
