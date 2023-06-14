package org.ergoplatform.uexplorer

import org.ergoplatform.uexplorer.{Address, BlockId, BlockMetadata, BoxId, Height, Value}

import scala.collection.immutable.TreeSet

trait Storage {

  def isEmpty: Boolean

  def containsBlock(blockId: BlockId, atHeight: Height): Boolean

  def findMissingHeights: TreeSet[Height]

  def getCurrentVersion: Long

  def getBlockById(blockId: BlockId): Option[BlockMetadata]

  def getLastHeight: Option[Height]

  def getLastBlocks: Map[BlockId, BlockMetadata]

  def getBlocksByHeight(atHeight: Height): Map[BlockId, BlockMetadata]

  def getAddressByUtxo(boxId: BoxId): Option[Address]

  def getUtxosByAddress(address: Address): Option[Map[BoxId, Value]]

}
