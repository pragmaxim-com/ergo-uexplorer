package org.ergoplatform.uexplorer

import org.ergoplatform.uexplorer.chain.ChainTip
import org.ergoplatform.uexplorer.db.BlockInfo
import org.ergoplatform.uexplorer.{BlockId, BoxId, ErgoTreeHex, Height, Value}

import java.nio.file.Path
import scala.collection.immutable.TreeSet
import scala.collection.concurrent
import scala.util.Try

trait Storage {

  def isEmpty: Boolean

  def getChainTip: Try[ChainTip]

  def containsBlock(blockId: BlockId, atHeight: Height): Boolean

  def findMissingHeights: TreeSet[Height]

  def getCurrentRevision: Long

  def getBlockById(blockId: BlockId): Option[BlockInfo]

  def getLastHeight: Option[Height]

  def getLastBlocks: Map[BlockId, BlockInfo]

  def getBlocksByHeight(atHeight: Height): Map[BlockId, BlockInfo]

  def getErgoTreeHexByUtxo(boxId: BoxId): Option[ErgoTreeHex]

}
