package org.ergoplatform.uexplorer

import org.ergoplatform.uexplorer.chain.ChainTip
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.{BlockId, BoxId, ErgoTreeHex, Height, Value}

import java.nio.file.Path
import scala.collection.concurrent
import scala.collection.immutable.TreeSet
import scala.util.Try

trait Storage {

  def isEmpty: Boolean

  def getChainTip: Try[ChainTip]

  def containsBlock(blockId: BlockId, atHeight: Height): Boolean

  def findMissingHeights: TreeSet[Height]

  def getCurrentRevision: Long

  def getBlockById(blockId: BlockId): Option[Block]

  def getLastHeight: Option[Height]

  def getLastBlocks: Map[BlockId, Block]

  def getBlocksByHeight(atHeight: Height): Map[BlockId, Block]

  def getErgoTreeHexByUtxo(boxId: BoxId): Option[ErgoTreeHex]

}
