package org.ergoplatform.uexplorer.utxo

import org.ergoplatform.uexplorer.{Address, BlockId, BlockMetadata, BoxId, Height, Value}

trait UtxoState {

  def isEmpty: Boolean

  def getLastBlock: Option[(Height, BlockMetadata)]

  def getAddressStats(address: Address): Option[Address.Stats]

  def containsBlock(blockId: BlockId, atHeight: Height): Boolean

  def getAddressByUtxo(boxId: BoxId): Option[Address]

  def getUtxosByAddress(address: Address): Option[Map[BoxId, Value]]

  def getTopAddresses: Iterator[(Address, Address.Stats)]

  def getBlocksByHeight: Iterator[(Height, BlockMetadata)]
}
