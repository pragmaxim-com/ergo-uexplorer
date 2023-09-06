package org.ergoplatform.uexplorer

import org.ergoplatform.uexplorer.chain.ChainTip
import org.ergoplatform.uexplorer.db.{Asset, Block, ErgoTree, ErgoTreeT8, LinkedBlock, OutputRecords, Utxo}
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.{BlockId, BoxId, ErgoTreeHex, Height, Value}
import zio.Task

import java.nio.file.Path
import scala.collection.compat.immutable.ArraySeq
import scala.collection.{concurrent, mutable}
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

  def writeReportAndCompact(indexing: Boolean): Task[Unit]

  def commit(): Task[Revision]

  def rollbackTo(rev: Revision): Task[Unit]

  def removeInputBoxesByErgoTree(inputIds: Seq[BoxId]): Task[_]

  def removeInputBoxesByErgoTreeT8(inputIds: Seq[BoxId]): Task[_]

  def removeInputBoxesByTokenId(inputIds: Seq[BoxId]): Task[_]

  def persistErgoTreeByUtxo(byErgoTree: Iterable[(ErgoTree, mutable.Set[Utxo])]): Task[_]

  def persistErgoTreeT8ByUtxo(byErgoTreeT8: Iterable[(ErgoTreeT8, mutable.Set[Utxo])]): Task[_]

  def persistTokensByUtxo(tokensByUtxo: mutable.Map[BoxId, mutable.Map[TokenId, Amount]]): Task[_]

  def persistUtxosByTokenId(utxosByTokenId: mutable.Map[TokenId, mutable.Set[BoxId]]): Task[_]

  def compact(indexing: Boolean): Task[Unit]

  def insertNewBlock(
    blockId: BlockId,
    block: Block,
    currentVersion: Revision
  ): Task[Set[BlockId]]
}
