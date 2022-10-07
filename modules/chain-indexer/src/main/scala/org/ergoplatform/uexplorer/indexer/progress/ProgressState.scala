package org.ergoplatform.uexplorer.indexer.progress

import org.ergoplatform.uexplorer.BlockId
import org.ergoplatform.uexplorer.db.{BlockInfo, Block}
import org.ergoplatform.uexplorer.indexer.{ProtocolSettings, UnexpectedStateError}
import org.ergoplatform.uexplorer.indexer.db.BlockBuilder
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor._
import org.ergoplatform.uexplorer.indexer.progress.ProgressState.BlockCache
import org.ergoplatform.uexplorer.node.ApiFullBlock

import scala.collection.immutable.{SortedMap, SortedSet, TreeMap, TreeSet}
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

case class ProgressState(
  lastBlockIdInEpoch: SortedMap[Int, BlockId],
  invalidEpochs: SortedMap[Int, InvalidEpochCandidate],
  blockCache: BlockCache
) {
  import ProgressState._

  def getFinishedEpoch(currentEpochIndex: Int): (MaybeNewEpoch, ProgressState) =
    if (lastBlockIdInEpoch.contains(currentEpochIndex)) {
      NewEpochExisted(currentEpochIndex) -> this
    } else {
      val previousEpochIndex = currentEpochIndex - 1
      val epochCandidate =
        EpochCandidate(
          Epoch
            .heightRangeForEpochIndex(currentEpochIndex)
            .map { height =>
              val info = blockCache.byHeight(height)
              height -> BlockRel(info.headerId, info.parentId)
            }
        )
      epochCandidate match {
        case Right(candidate) if currentEpochIndex == 0 =>
          val newEpoch = candidate.getEpoch
          val newP     = copy(lastBlockIdInEpoch = lastBlockIdInEpoch.updated(newEpoch.index, newEpoch.blockIds.last))
          NewEpochCreated(newEpoch) -> newP
        case Right(candidate) if lastBlockIdInEpoch(previousEpochIndex) == candidate.relsByHeight.head._2.parentId =>
          def removeEpochFromCache(heightsToRemove: Seq[Int]): BlockCache =
            BlockCache(
              blockCache.byId -- heightsToRemove.flatMap(blockCache.byHeight.get).map(_.headerId),
              blockCache.byHeight -- heightsToRemove
            )

          val newBlockCache = removeEpochFromCache(Epoch.heightRangeForEpochIndex(previousEpochIndex))
          val newEpoch      = candidate.getEpoch
          val newP =
            ProgressState(
              lastBlockIdInEpoch.updated(newEpoch.index, newEpoch.blockIds.last),
              invalidEpochs,
              newBlockCache
            )
          NewEpochCreated(newEpoch) -> newP
        case Right(candidate) =>
          val Epoch(curIndex, curHeaders) = candidate.getEpoch
          val error =
            s"Prev epoch $previousEpochIndex header ${lastBlockIdInEpoch.get(previousEpochIndex)} " +
            s"does not match current epoch $curIndex header ${curHeaders.head}"
          val invalidHeights =
            TreeSet(Epoch.heightRangeForEpochIndex(previousEpochIndex).last, candidate.relsByHeight.head._1)
          val invalidEpochCandidate = InvalidEpochCandidate(curIndex, invalidHeights, error)
          val newP                  = copy(invalidEpochs = invalidEpochs.updated(candidate.epochIndex, invalidEpochCandidate))
          NewEpochFailed(invalidEpochCandidate) -> newP
        case Left(candidate) =>
          NewEpochFailed(candidate) -> copy(invalidEpochs = invalidEpochs.updated(candidate.epochIndex, candidate))
      }
    }

  def insertBestBlock(
    bestBlock: ApiFullBlock
  )(implicit protocol: ProtocolSettings): Try[(BestBlockInserted, ProgressState)] =
    if (!hasParent(bestBlock)) {
      Failure(
        new UnexpectedStateError(
          s"Inserting block ${bestBlock.header.id} at ${bestBlock.header.height} without parent being applied is not possible"
        )
      )
    } else
      BlockBuilder(bestBlock, blockCache.byId.get(bestBlock.header.parentId))
        .map { flatBlock =>
          BestBlockInserted(flatBlock) -> copy(blockCache =
            BlockCache(
              blockCache.byId.updated(flatBlock.header.id, CachedBlock.fromBlock(flatBlock)),
              blockCache.byHeight.updated(flatBlock.header.height, CachedBlock.fromBlock(flatBlock))
            )
          )
        }

  def insertWinningFork(
    winningFork: List[ApiFullBlock]
  )(implicit protocol: ProtocolSettings): Try[(ForkInserted, ProgressState)] =
    if (!hasParentAndIsChained(winningFork)) {
      Failure(
        new UnexpectedStateError(
          s"Inserting fork ${winningFork.map(_.header.id).mkString(",")} at height ${winningFork.map(_.header.height).mkString(",")} illegal"
        )
      )
    } else
      winningFork
        .foldLeft(Try(ListBuffer.empty[Block] -> ListBuffer.empty[CachedBlock])) {
          case (f @ Failure(_), _) =>
            f
          case (Success((newBlocksAcc, toRemoveAcc)), apiBlock) =>
            BlockBuilder(
              apiBlock,
              Some(
                newBlocksAcc.lastOption
                  .collect { case b if b.header.id == apiBlock.header.parentId => CachedBlock.fromBlock(b) }
                  .getOrElse(blockCache.byId(apiBlock.header.parentId))
              )
            ).map { newBlock =>
              val newBlocks = newBlocksAcc :+ newBlock
              val toRemove =
                toRemoveAcc ++ blockCache.byHeight
                  .get(apiBlock.header.height)
                  .filter(_.headerId != apiBlock.header.id)
              newBlocks -> toRemove
            }
        }
        .map { case (newBlocks, supersededBlocks) =>
          val newBlockCache =
            BlockCache(
              (blockCache.byId -- supersededBlocks.map(_.headerId)) ++ newBlocks
                .map(b => b.header.id -> CachedBlock.fromBlock(b)),
              blockCache.byHeight ++ newBlocks.map(b => b.header.height -> CachedBlock.fromBlock(b))
            )
          ForkInserted(newBlocks.toList, supersededBlocks.toList) -> copy(blockCache = newBlockCache)
        }

  def updateState(persistedEpochIndexes: TreeMap[Int, CachedBlock]): ProgressState = {
    val newEpochIndexes = lastBlockIdInEpoch ++ persistedEpochIndexes.view.mapValues(_.headerId)
    val newBlockCache =
      BlockCache(
        blockCache.byId ++ persistedEpochIndexes.toSeq.map(i => i._2.headerId -> i._2),
        blockCache.byHeight ++ persistedEpochIndexes.map(i => i._2.height -> i._2)
      )
    ProgressState(newEpochIndexes, invalidEpochs, newBlockCache)
  }

  def getLastCachedBlock: Option[CachedBlock] = blockCache.byHeight.lastOption.map(_._2)

  def persistedEpochIndexes: SortedSet[Int] = lastBlockIdInEpoch.keySet

  /** Genesis block is not part of a cache as it has no parent so
    * we assert that any block either has its parent cached or its a first block
    */
  def hasParent(block: ApiFullBlock): Boolean =
    blockCache.byId.contains(block.header.parentId) || block.header.height == 1

  def hasParentAndIsChained(fork: List[ApiFullBlock]): Boolean =
    fork.size > 1 &&
    blockCache.byId.contains(fork.head.header.parentId) &&
    fork.sliding(2).forall {
      case first :: second :: Nil =>
        first.header.id == second.header.parentId
      case _ =>
        false
    }

  def findMissingIndexes: TreeSet[Int] =
    if (lastBlockIdInEpoch.isEmpty || lastBlockIdInEpoch.size == 1)
      TreeSet.empty
    else
      TreeSet((lastBlockIdInEpoch.head._1 to lastBlockIdInEpoch.last._1): _*)
        .diff(lastBlockIdInEpoch.keySet)

  override def toString: String = {
    val existingEpochs = persistedEpochIndexes
    val cachedHeights  = blockCache.heights

    def headStr(xs: SortedSet[Int]) = xs.headOption.map(h => s"[$h").getOrElse("")

    def lastStr(xs: SortedSet[Int]) =
      if (xs.isEmpty) ""
      else xs.lastOption.filterNot(xs.headOption.contains).map(h => s" - $h]").getOrElse("]")

    s"persisted Epochs: ${existingEpochs.size}${headStr(existingEpochs)}${lastStr(existingEpochs)}, " +
    s"blocks cache size (heights): ${cachedHeights.size}${headStr(cachedHeights)}${lastStr(cachedHeights)}, " +
    s"invalid Epochs: ${invalidEpochs.size}${headStr(invalidEpochs.keySet)}${lastStr(invalidEpochs.keySet)}"
  }

}

object ProgressState {

  case class CachedBlock(headerId: BlockId, parentId: BlockId, timestamp: Long, height: Int, info: BlockInfo)

  object CachedBlock {

    def fromBlock(b: Block): CachedBlock =
      CachedBlock(b.header.id, b.header.parentId, b.header.timestamp, b.header.height, b.info)
  }

  case class BlockCache(byId: Map[BlockId, CachedBlock], byHeight: SortedMap[Int, CachedBlock]) {
    def isEmpty: Boolean = byId.isEmpty || byHeight.isEmpty

    def heights: SortedSet[Int] = byHeight.keySet
  }

}
