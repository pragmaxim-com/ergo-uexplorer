package org.ergoplatform.uexplorer

import org.ergoplatform.uexplorer.db.VersionedBlock
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId, Height, Value}

import scala.collection.immutable.TreeSet

trait Storage {

  def isEmpty: Boolean

  def containsBlock(blockId: BlockId, atHeight: Height): Boolean

  def findMissingHeights: TreeSet[Height]

  def getCurrentVersion: Long

  def getBlockById(blockId: BlockId): Option[VersionedBlock]

  def getLastHeight: Option[Height]

  def getLastBlocks: Map[BlockId, VersionedBlock]

  def getBlocksByHeight(atHeight: Height): Map[BlockId, VersionedBlock]

  def getAddressByUtxo(boxId: BoxId): Option[Address]

  def getUtxosByAddress(address: Address): Option[Map[BoxId, Value]]

}
