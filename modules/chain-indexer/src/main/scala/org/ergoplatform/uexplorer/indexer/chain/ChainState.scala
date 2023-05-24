package org.ergoplatform.uexplorer.indexer.chain

import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.db.{Block, BlockInfo}
import org.ergoplatform.uexplorer.indexer.chain.ChainState.BlockBuffer
import org.ergoplatform.uexplorer.indexer.chain.ChainStateHolder.*
import org.ergoplatform.uexplorer.db.BlockBuilder
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState
import org.ergoplatform.uexplorer.MapPimp
import org.ergoplatform.uexplorer.node.ApiFullBlock

import scala.collection.immutable.{ArraySeq, List, SortedMap, SortedSet, TreeMap, TreeSet}
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

case class ChainState(
  lastBlockIdInEpoch: SortedMap[EpochIndex, BlockId],
  blockBuffer: BlockBuffer,
  utxoState: UtxoState
) {
  import ChainState.*

  def finishEpoch(currentEpochIndex: EpochIndex): Try[(MaybeNewEpoch, ChainState)] =
    if (lastBlockIdInEpoch.contains(currentEpochIndex)) {
      Success(NewEpochExisted(currentEpochIndex) -> this)
    } else {
      val previousEpochIndex = currentEpochIndex - 1
      val heightRange        = Epoch.heightRangeForEpochIndex(currentEpochIndex)
      EpochCandidate(blockBuffer.blockRelationsByHeight(heightRange)) match {
        case Right(candidate)
            if currentEpochIndex == 0 || lastBlockIdInEpoch(previousEpochIndex) == candidate.relsByHeight.head._2.parentId =>
          val newEpoch = candidate.getEpoch
          Try(utxoState.mergeBufferedBoxes(Option(heightRange)))
            .map { case (boxesByHeight, newState) =>
              NewEpochDetected(newEpoch, boxesByHeight, newState.topAddresses.nodeMap) -> ChainState(
                lastBlockIdInEpoch.updated(newEpoch.index, newEpoch.blockIds.last),
                blockBuffer.flushEpoch(heightRange),
                newState
              )
            }
        case Right(candidate) =>
          val Epoch(curIndex, _) = candidate.getEpoch
          val invalidHeights =
            TreeSet(Epoch.heightRangeForEpochIndex(previousEpochIndex).last, candidate.relsByHeight.head._1)
          val error =
            s"Prev epoch $previousEpochIndex header ${lastBlockIdInEpoch.get(previousEpochIndex)} " +
              s"does not match current epoch $curIndex header, invalid heights: ${invalidHeights.mkString(",")}"
          Failure(new IllegalStateException(error))
        case Left(candidate) =>
          Failure(new IllegalStateException(candidate.error))
      }
    }

  def insertBestBlock(
    b: ApiFullBlock
  )(implicit protocol: ProtocolSettings): Try[(BestBlockInserted, ChainState)] =
    if (!hasParent(b)) {
      Failure(
        new UnexpectedStateError(
          s"Inserting block ${b.header.id} at ${b.header.height} without parent being applied is not possible"
        )
      )
    } else
      BlockBuilder(b, blockBuffer.byId.get(b.header.parentId))
        .map { block =>
          BestBlockInserted(block) -> copy(
            blockBuffer = blockBuffer.addBlock(block),
            utxoState =
              utxoState.insertBestBlock(b.header.id, b.header.height, b.header.timestamp, b.transactions.transactions)
          )
        }

  def insertWinningFork(
    winningFork: List[ApiFullBlock]
  )(implicit protocol: ProtocolSettings): Try[(ForkInserted, ChainState)] =
    if (!hasParentAndIsChained(winningFork)) {
      Failure(
        new UnexpectedStateError(
          s"Inserting fork ${winningFork.map(_.header.id).mkString(",")} at height ${winningFork.map(_.header.height).mkString(",")} illegal"
        )
      )
    } else
      winningFork
        .foldLeft(Try((ListBuffer.empty[ApiFullBlock], ListBuffer.empty[Block], ListBuffer.empty[BlockMetadata]))) {
          case (f @ Failure(_), _) =>
            f
          case (Success((newApiBlocksAcc, newBlocksAcc, toRemoveAcc)), apiBlock) =>
            BlockBuilder(
              apiBlock,
              Some(
                newBlocksAcc.lastOption
                  .collect { case b if b.header.id == apiBlock.header.parentId => BlockMetadata.fromBlock(b) }
                  .getOrElse(blockBuffer.byId(apiBlock.header.parentId))
              )
            ).map { newBlock =>
              val newApiBlocks = newApiBlocksAcc :+ apiBlock
              val newBlocks    = newBlocksAcc :+ newBlock
              val toRemove =
                toRemoveAcc ++ blockBuffer.byHeight
                  .get(apiBlock.header.height)
                  .filter(_.headerId != apiBlock.header.id)
              (newApiBlocks, newBlocks, toRemove)
            }
        }
        .map { case (newApiBlocks, newBlocks, supersededBlocks) =>
          ForkInserted(newBlocks.toList, supersededBlocks.toList) -> copy(
            blockBuffer = blockBuffer.addFork(newBlocks, supersededBlocks),
            utxoState   = utxoState.insertFork(newApiBlocks, supersededBlocks)
          )
        }

  def getLastCachedBlock: Option[BlockMetadata] = blockBuffer.byHeight.lastOption.map(_._2)

  def persistedEpochIndexes: SortedSet[EpochIndex] = lastBlockIdInEpoch.keySet

  /** Genesis block is not part of a cache as it has no parent so we assert that any block either has its parent cached or
    * its a first block
    */
  def hasParent(block: ApiFullBlock): Boolean =
    blockBuffer.byId.contains(block.header.parentId) || block.header.height == 1

  def hasParentAndIsChained(fork: List[ApiFullBlock]): Boolean =
    fork.size > 1 &&
      blockBuffer.byId.contains(fork.head.header.parentId) &&
      fork.sliding(2).forall {
        case first :: second :: Nil =>
          first.header.id == second.header.parentId
        case _ =>
          false
      }

  def findMissingEpochIndexes: TreeSet[EpochIndex] =
    if (lastBlockIdInEpoch.isEmpty || lastBlockIdInEpoch.size == 1)
      TreeSet.empty
    else
      TreeSet((lastBlockIdInEpoch.head._1 to lastBlockIdInEpoch.last._1): _*)
        .diff(lastBlockIdInEpoch.keySet)

  override def toString: String = {
    val existingEpochs = persistedEpochIndexes
    val cachedHeights  = blockBuffer.heights

    def headStr(xs: SortedSet[Int]) = xs.headOption.map(h => s"[$h").getOrElse("")

    def lastStr(xs: SortedSet[Int]) =
      if (xs.isEmpty) ""
      else xs.lastOption.filterNot(xs.headOption.contains).map(h => s" - $h]").getOrElse("]")

    s"utxo count: ${utxoState.addressByUtxo.size}, non-empty-address count: ${utxoState.utxosByAddress.size}, " +
    s"active-address count: ${utxoState.topAddresses.nodeMap.size}, " +
    s"persisted Epochs: ${existingEpochs.size}${headStr(existingEpochs)}${lastStr(existingEpochs)}, " +
    s"blocks cache size (heights): ${cachedHeights.size}${headStr(cachedHeights)}${lastStr(cachedHeights)}"
  }

}

object ChainState {

  def empty: ChainState = apply(TreeMap.empty, UtxoState.empty)

  def apply(bufferedInfoByEpochIndex: TreeMap[Int, BlockMetadata], utxoState: UtxoState): ChainState =
    ChainState(
      bufferedInfoByEpochIndex.map { case (epochIndex, blockInfo) => epochIndex -> blockInfo.headerId },
      BlockBuffer(
        bufferedInfoByEpochIndex.toSeq.map(i => i._2.headerId -> i._2).toMap,
        bufferedInfoByEpochIndex.map(i => i._2.height -> i._2)
      ),
      utxoState
    )

  case class BlockBuffer(byId: Map[BlockId, BlockMetadata], byHeight: SortedMap[Height, BlockMetadata]) {
    def isEmpty: Boolean = byId.isEmpty || byHeight.isEmpty

    def heights: SortedSet[Height] = byHeight.keySet

    def addBlock(block: Block): BlockBuffer =
      BlockBuffer(
        byId.updated(block.header.id, BlockMetadata.fromBlock(block)),
        byHeight.updated(block.header.height, BlockMetadata.fromBlock(block))
      )

    def addFork(newFork: ListBuffer[Block], supersededFork: ListBuffer[BlockMetadata]): BlockBuffer =
      BlockBuffer(
        (byId -- supersededFork.map(_.headerId)) ++ newFork
          .map(b => b.header.id -> BlockMetadata.fromBlock(b)),
        byHeight ++ newFork.map(b => b.header.height -> BlockMetadata.fromBlock(b))
      )

    def blockRelationsByHeight(heightRange: Seq[Height]): Seq[(Height, BlockRel)] =
      heightRange
        .map { height =>
          val info = byHeight(height)
          height -> BlockRel(info.headerId, info.parentId)
        }

    def flushEpoch(heightRange: Seq[Height]): BlockBuffer =
      BlockBuffer(
        byId -- heightRange.flatMap(byHeight.get).map(_.headerId),
        byHeight -- heightRange
      )
  }

}
