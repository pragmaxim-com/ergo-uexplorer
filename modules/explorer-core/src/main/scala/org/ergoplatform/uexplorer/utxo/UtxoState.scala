package org.ergoplatform.uexplorer.utxo

import org.ergoplatform.uexplorer.{Address, BlockId, BlockMetadata, BoxId, Height, Value}

import scala.collection.immutable.TreeSet

trait UtxoState {

  def isEmpty: Boolean

  def getLastHeight: Option[Height]

  def getLastBlocks: Map[BlockId, BlockMetadata]

  def getAddressStats(address: Address): Option[Address.Stats]

  def containsBlock(blockId: BlockId, atHeight: Height): Boolean

  def getAddressByUtxo(boxId: BoxId): Option[Address]

  def getUtxosByAddress(address: Address): Option[Map[BoxId, Value]]

  def findMissingHeights: TreeSet[Height]

}
