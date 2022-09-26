package org.ergoplatform.uexplorer.indexer.progress

import org.ergoplatform.explorer.BlockId
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.explorer.protocol.models.ApiFullBlock
import org.ergoplatform.explorer.settings.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.UnexpectedStateError
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor._
import org.ergoplatform.uexplorer.indexer.progress.ProgressState.BlockCache

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
              height -> BlockRel(info.stats.headerId, info.parentId)
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
              blockCache.byId -- heightsToRemove.flatMap(blockCache.byHeight.get).map(_.stats.headerId),
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
      buildBlock(bestBlock, blockCache.byId.get(bestBlock.header.parentId).map(_.stats))
        .map { flatBlock =>
          val newBlockInfo = flatBlock.buildInfo
          BestBlockInserted(flatBlock) -> copy(blockCache =
            BlockCache(
              blockCache.byId.updated(flatBlock.header.id, newBlockInfo),
              blockCache.byHeight.updated(flatBlock.header.height, newBlockInfo)
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
        .foldLeft(Try(ListBuffer.empty[FlatBlock] -> ListBuffer.empty[BlockInfo])) {
          case (f @ Failure(_), _) =>
            f
          case (Success((newBlocksAcc, toRemoveAcc)), apiBlock) =>
            buildBlock(
              apiBlock,
              Some(
                newBlocksAcc.lastOption
                  .collect { case b if b.header.id == apiBlock.header.parentId => b.info }
                  .getOrElse(blockCache.byId(apiBlock.header.parentId).stats)
              )
            ).map { newBlock =>
              val newBlocks = newBlocksAcc :+ newBlock
              val toRemove =
                toRemoveAcc ++ blockCache.byHeight
                  .get(apiBlock.header.height)
                  .filter(_.stats.headerId != apiBlock.header.id)
              newBlocks -> toRemove
            }
        }
        .map { case (newBlocks, supersededBlocks) =>
          val newBlockCache =
            BlockCache(
              (blockCache.byId -- supersededBlocks.map(_.stats.headerId)) ++ newBlocks
                .map(b => b.header.id -> b.buildInfo),
              blockCache.byHeight ++ newBlocks.map(b => b.header.height -> b.buildInfo)
            )
          ForkInserted(newBlocks.toList, supersededBlocks.toList) -> copy(blockCache = newBlockCache)
        }

  def updateState(persistedEpochIndexes: TreeMap[Int, BlockInfo]): ProgressState = {
    val newEpochIndexes = lastBlockIdInEpoch ++ persistedEpochIndexes.mapValues(_.stats.headerId)
    val newBlockCache =
      BlockCache(
        blockCache.byId ++ persistedEpochIndexes.map(i => i._2.stats.headerId -> i._2),
        blockCache.byHeight ++ persistedEpochIndexes.map(i => i._2.stats.height -> i._2)
      )
    ProgressState(newEpochIndexes, invalidEpochs, newBlockCache)
  }

  def getLastCachedBlock: Option[BlockInfo] = blockCache.byHeight.lastOption.map(_._2)

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

  import cats.Applicative
  import org.ergoplatform.explorer.BlockId
  import org.ergoplatform.explorer.BuildFrom.syntax._
  import org.ergoplatform.explorer.db.models.BlockStats
  import org.ergoplatform.explorer.indexer.extractors._
  import org.ergoplatform.explorer.indexer.models.{FlatBlock, SlotData}
  import org.ergoplatform.explorer.protocol.models.ApiFullBlock
  import org.ergoplatform.explorer.settings.ProtocolSettings
  import tofu.Context

  import scala.util.Try

  case class BlockCache(byId: Map[BlockId, BlockInfo], byHeight: SortedMap[Int, BlockInfo]) {
    def isEmpty: Boolean = byId.isEmpty || byHeight.isEmpty

    def heights: SortedSet[Int] = byHeight.keySet
  }

  case class BlockInfo(parentId: BlockId, stats: BlockStats)

  def updateTotalStats(currentBlockStats: BlockStats, prevBlockStats: BlockStats)(implicit
    protocol: ProtocolSettings
  ): BlockStats =
    currentBlockStats.copy(
      blockChainTotalSize = prevBlockStats.blockChainTotalSize + currentBlockStats.blockSize,
      totalTxsCount       = prevBlockStats.totalTxsCount + currentBlockStats.txsCount,
      totalCoinsIssued    = protocol.emission.issuedCoinsAfterHeight(currentBlockStats.height),
      totalMiningTime     = prevBlockStats.totalMiningTime + (currentBlockStats.timestamp - prevBlockStats.timestamp),
      totalFees           = prevBlockStats.totalFees + currentBlockStats.blockFee,
      totalMinersReward   = prevBlockStats.totalMinersReward + currentBlockStats.minerReward,
      totalCoinsInTxs     = prevBlockStats.totalCoinsInTxs + currentBlockStats.blockCoins
    )

  def updateMainChain(block: FlatBlock, mainChain: Boolean, prevBlockInfoOpt: Option[BlockStats])(implicit
    protocol: ProtocolSettings
  ): FlatBlock = {
    import monocle.macros.syntax.lens._
    block
      .lens(_.header.mainChain)
      .modify(_ => mainChain)
      .lens(_.info.mainChain)
      .modify(_ => mainChain)
      .lens(_.txs)
      .modify(_.map(_.copy(mainChain = mainChain)))
      .lens(_.inputs)
      .modify(_.map(_.copy(mainChain = mainChain)))
      .lens(_.dataInputs)
      .modify(_.map(_.copy(mainChain = mainChain)))
      .lens(_.outputs)
      .modify(_.map(_.copy(mainChain = mainChain)))
      .lens(_.info)
      .modify {
        case currentBlockStats if prevBlockInfoOpt.nonEmpty =>
          updateTotalStats(currentBlockStats, prevBlockInfoOpt.get)
        case currentBlockStats =>
          currentBlockStats
      }
  }

  def buildBlock(apiBlock: ApiFullBlock, prevBlockInfo: Option[BlockStats])(implicit
    protocol: ProtocolSettings
  ): Try[FlatBlock] = {
    implicit val ctx = Context.const(protocol)(Applicative[Try])
    SlotData(apiBlock, prevBlockInfo)
      .intoF[Try, FlatBlock]
      .map(updateMainChain(_, mainChain = true, prevBlockInfo))
  }

  implicit class FlatBlockPimp(underlying: FlatBlock) {
    def buildInfo: BlockInfo = BlockInfo(underlying.header.parentId, underlying.info)
  }

}
