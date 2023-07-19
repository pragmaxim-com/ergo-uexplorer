package org.ergoplatform.uexplorer

import org.ergoplatform.uexplorer.chain.ChainTip
import org.ergoplatform.uexplorer.db.{Block, InputRecords, OutputRecords}
import org.ergoplatform.uexplorer.{BlockId, BoxId, ErgoTreeHex, Height, Value}
import zio.Task

import java.nio.file.Path
import scala.collection.concurrent
import scala.collection.immutable.TreeSet
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

}

trait WritableStorage extends ReadableStorage {

  def writeReportAndCompact(blocksIndexed: Int): Task[Unit]

  def commit(): Revision

  def rollbackTo(rev: Revision): Unit

  def removeInputBoxesByErgoTree(inputRecords: InputRecords): Task[_]

  def removeInputBoxesByErgoTreeT8(inputRecords: InputRecords): Task[_]

  def persistErgoTreeT8Utxos(outputRecords: OutputRecords): Task[_]

  def persistErgoTreeUtxos(outputRecords: OutputRecords): Task[_]

  def compact(indexing: Boolean): Task[Unit]

  def insertNewBlock(
    blockId: BlockId,
    block: Block,
    currentVersion: Revision
  ): Task[Set[BlockId]]
}
