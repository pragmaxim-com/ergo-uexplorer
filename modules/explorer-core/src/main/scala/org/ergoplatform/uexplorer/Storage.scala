package org.ergoplatform.uexplorer

import org.ergoplatform.uexplorer.db.BlockInfo
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId, Height, Value}

import scala.collection.immutable.TreeSet
import scala.collection.concurrent
import scala.util.Try

trait Storage {

  def writeReport: Try[_]

  def isEmpty: Boolean

  def containsBlock(blockId: BlockId, atHeight: Height): Boolean

  def findMissingHeights: TreeSet[Height]

  def getCurrentRevision: Long

  def getBlockById(blockId: BlockId): Option[BlockInfo]

  def getLastHeight: Option[Height]

  def getLastBlocks: Map[BlockId, BlockInfo]

  def getBlocksByHeight(atHeight: Height): Map[BlockId, BlockInfo]

  def getAddressByUtxo(boxId: BoxId): Option[Address]

  def getUtxosByAddress(address: Address): Option[java.util.Map[BoxId, Value]]

  def getUtxoValueByAddress(address: Address, utxo: BoxId): Option[Value]
}
