package org.ergoplatform.uexplorer

import org.ergoplatform.uexplorer.db.BlockInfo
import org.ergoplatform.uexplorer.{BlockId, BoxId, ErgoTreeHex, Height, Value}

import scala.collection.immutable.TreeSet
import scala.collection.concurrent
import scala.util.Try

trait Storage {

  def isEmpty: Boolean

  def containsBlock(blockId: BlockId, atHeight: Height): Boolean

  def findMissingHeights: TreeSet[Height]

  def getCurrentRevision: Long

  def getBlockById(blockId: BlockId): Option[BlockInfo]

  def getLastHeight: Option[Height]

  def getLastBlocks: Map[BlockId, BlockInfo]

  def getBlocksByHeight(atHeight: Height): Map[BlockId, BlockInfo]

  def getErgoTreeHexByUtxo(boxId: BoxId): Option[ErgoTreeHex]

  def getUtxosByErgoTreeHex(ergoTreeHex: ErgoTreeHex): Option[java.util.Map[BoxId, Value]]

  def getUtxoValueByErgoTreeHex(ergoTreeHex: ErgoTreeHex, utxo: BoxId): Option[Value]

  def getUtxoValuesByErgoTreeHex(ergoTreeHex: ErgoTreeHex, utxos: IterableOnce[BoxId]): Option[java.util.Map[BoxId, Value]]

  def getUtxoValuesByErgoTreeT8Hex(
    ergoTreeHex: ErgoTreeT8Hex,
    utxos: IterableOnce[BoxId]
  ): Option[java.util.Map[BoxId, CreationHeight]]
}
