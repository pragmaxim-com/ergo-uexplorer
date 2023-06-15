package org.ergoplatform.uexplorer

import org.ergoplatform.uexplorer.db.BlockInfo
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId, Height, Value}

import scala.collection.immutable.TreeSet

trait Storage {

  def isEmpty: Boolean

  def containsBlock(blockId: BlockId, atHeight: Height): Boolean

  def findMissingHeights: TreeSet[Height]

  def getCurrentVersion: Long

  def getBlockById(blockId: BlockId): Option[BlockInfo]

  def getLastHeight: Option[Height]

  def getLastBlocks: Map[BlockId, BlockInfo]

  def getBlocksByHeight(atHeight: Height): Map[BlockId, BlockInfo]

  def getAddressByUtxo(boxId: BoxId): Option[Address]

  def getUtxosByAddress(address: Address): Option[Map[BoxId, Value]]

}

object Storage {
  type StorageVersion = Long
}
