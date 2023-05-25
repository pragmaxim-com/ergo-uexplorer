package org.ergoplatform.uexplorer.utxo

import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.utxo.UtxoState.BestBlock

import scala.collection.mutable.ListBuffer

trait UtxoStateLike[T] {

  def addBestBlock(bestBlock: BestBlock): T
  def addFork(newApiBlocks: ListBuffer[BestBlock], supersededBlocks: ListBuffer[BlockMetadata]): T
  def getAddressByUtxo(boxId: BoxId): Option[Address]
  def getUtxosByAddress(address: Address): Option[Map[BoxId, Value]]
  def topAddresses: TopAddresses

}
