package org.ergoplatform.uexplorer

import org.ergoplatform.uexplorer.chain.ChainTip
import org.ergoplatform.uexplorer.db.{Block, OutputRecords}
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.{BlockId, BoxId, ErgoTreeHex, Height, Value}
import zio.Task

import java.nio.file.Path
import scala.collection.compat.immutable.ArraySeq
import scala.collection.concurrent
import scala.collection.immutable.{ArraySeq, TreeSet}
import scala.util.Try

trait ReadableStorage {

  def isEmpty: Boolean

  def getChainTip: Task[ChainTip]

  def containsBlock(blockId: BlockId, atHeight: Height): Boolean

  def findMissingHeights: TreeSet[Height]

  def getCurrentRevision: Long

  def getBlockById(blockId: BlockId): Option[Block]

  def getLastHeight: Option[Height]

  def getLastBlocks: Map[BlockId, Block]

  def getBlocksByHeight(atHeight: Height): Map[BlockId, Block]

  def getErgoTreeHexByUtxo(boxId: BoxId): Option[ErgoTreeHex]

  def getErgoTreeT8HexByUtxo(boxId: BoxId): Option[ErgoTreeT8Hex]
}

trait WritableStorage extends ReadableStorage {

  def writeReportAndCompact(blocksIndexed: Int): Task[Unit]

  def commit(): Revision

  def rollbackTo(rev: Revision): Unit

  def removeInputBoxesByErgoTree(transactions: ArraySeq[ApiTransaction]): Task[_]

  def removeInputBoxesByErgoTreeT8(transactions: ArraySeq[ApiTransaction]): Task[_]

  def persistErgoTreeByUtxo(outputRecords: OutputRecords): Task[_]

  def persistErgoTreeT8ByUtxo(outputRecords: OutputRecords): Task[_]

  def compact(indexing: Boolean): Task[Unit]

  def insertNewBlock(
    blockId: BlockId,
    block: Block,
    currentVersion: Revision
  ): Task[Set[BlockId]]
}
