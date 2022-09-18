package org.ergoplatform.uexplorer.indexer.progress

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.pattern.StatusReply
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.BlockId
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.explorer.protocol.models.ApiFullBlock
import org.ergoplatform.explorer.settings.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient._
import org.ergoplatform.uexplorer.indexer.{Const, UnexpectedStateError}

import scala.collection.immutable.{SortedMap, SortedSet, TreeMap, TreeSet}
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class ProgressMonitor(implicit protocol: ProtocolSettings) extends LazyLogging {
  import ProgressMonitor._

  def initialBehavior: Behavior[ProgressMonitorRequest] =
    Behaviors.setup[ProgressMonitorRequest] { _ =>
      logger.info(s"Starting epoch monitoring ...")
      initialized(ProgressState(TreeMap.empty, TreeSet.empty, BlockCache(Map.empty, TreeMap.empty)))
    }

  def initialized(p: ProgressState): Behaviors.Receive[ProgressMonitorRequest] =
    Behaviors.receiveMessage[ProgressMonitorRequest] {
      case InsertBestBlock(bestBlock, replyTo) =>
        p.insertBestBlock(bestBlock) match {
          case Success((bestBlockInserted, newProgress)) =>
            replyTo ! StatusReply.success(bestBlockInserted)
            initialized(newProgress)
          case Failure(ex) =>
            val h = bestBlock.header
            logger.warn(s"Unexpected insert ${h.id} at ${h.height}, parent ${h.parentId} : $p", ex)
            replyTo ! StatusReply.error(ex)
            Behaviors.same
        }
      case InsertWinningFork(fork, replyTo) =>
        p.insertWinningFork(fork) match {
          case Success((winningForkInserted, newProgress)) =>
            replyTo ! StatusReply.success(winningForkInserted)
            initialized(newProgress)
          case Failure(ex) =>
            val h = fork.head.header
            logger.warn(
              s"Unexpected fork size ${fork.size} starting ${h.id} at ${h.height}, parent ${h.parentId} : $p",
              ex
            )
            replyTo ! StatusReply.error(ex)
            Behaviors.same
        }
      case GetLastBlock(replyTo) =>
        if (p.blockCache.byHeight.nonEmpty)
          replyTo ! StatusReply.success(LastCachedBlock(p.blockCache.byHeight.last._2))
        else
          replyTo ! StatusReply.error("Asking for last block should happen during polling phase")
        Behaviors.same
      case GetBlock(blockId, replyTo) =>
        replyTo ! CachedBlock(p.blockCache.byId.get(blockId))
        Behaviors.same
      case UpdateEpochIndexes(persistedEpochIndexes, replyTo) =>
        val newProgress = p.updateEpochIndexes(persistedEpochIndexes)
        replyTo ! newProgress
        logger.info(s"$newProgress")
        initialized(newProgress)
      case GetChainState(replyTo) =>
        replyTo ! p
        Behaviors.same
      case GetFinishedEpoch(epochIndex, replyTo) =>
        val (maybeNewEpoch, newProgress) = p.getFinishedEpoch(epochIndex)
        logger.info(s"$newProgress")
        replyTo ! maybeNewEpoch
        initialized(newProgress)
    }
}

object ProgressMonitor {

  sealed trait ProgressMonitorRequest

  sealed trait Insertable extends ProgressMonitorRequest {
    def replyTo: ActorRef[StatusReply[Inserted]]
  }

  case class InsertBestBlock(block: ApiFullBlock, replyTo: ActorRef[StatusReply[Inserted]]) extends Insertable

  case class InsertWinningFork(blocks: List[ApiFullBlock], replyTo: ActorRef[StatusReply[Inserted]]) extends Insertable

  case class GetBlock(blockId: BlockId, replyTo: ActorRef[CachedBlock]) extends ProgressMonitorRequest

  case class GetLastBlock(replyTo: ActorRef[StatusReply[LastCachedBlock]]) extends ProgressMonitorRequest

  case class UpdateEpochIndexes(lastBlockIdByEpochIndex: TreeMap[Int, BlockInfo], replyTo: ActorRef[ProgressState])
    extends ProgressMonitorRequest

  case class GetChainState(replyTo: ActorRef[ProgressState]) extends ProgressMonitorRequest

  case class GetFinishedEpoch(epochIndex: Int, replyTo: ActorRef[MaybeNewEpoch]) extends ProgressMonitorRequest

  sealed trait ProgressMonitorResponse

  case class LastCachedBlock(block: BlockInfo) extends ProgressMonitorResponse
  case class CachedBlock(block: Option[BlockInfo]) extends ProgressMonitorResponse

  sealed trait Inserted extends ProgressMonitorResponse

  case class BestBlockInserted(flatBlock: FlatBlock) extends Inserted

  case class ForkInserted(newFork: List[FlatBlock], supersededFork: List[BlockInfo]) extends Inserted

  sealed trait MaybeNewEpoch extends ProgressMonitorResponse

  case class NewEpochCreated(epochCandidate: Epoch) extends MaybeNewEpoch

  case class NewEpochFailed(epochCandidate: InvalidEpochCandidate) extends MaybeNewEpoch

  case class NewEpochExisted(epochIndex: Int) extends MaybeNewEpoch

  case class BlockCache(byId: Map[BlockId, BlockInfo], byHeight: SortedMap[Int, BlockInfo]) {
    def isEmpty: Boolean = byId.isEmpty || byHeight.isEmpty

    def heights: SortedSet[Int] = byHeight.keySet
  }

  case class ProgressState(
    lastBlockIdInEpoch: SortedMap[Int, BlockId],
    invalidIndexes: SortedSet[Int],
    blockCache: BlockCache
  ) extends ProgressMonitorResponse {
    def isCacheEmpty: Boolean = lastBlockIdInEpoch.isEmpty || blockCache.isEmpty

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
                invalidIndexes,
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
            val newP = copy(invalidIndexes = invalidIndexes + candidate.epochIndex)
            NewEpochFailed(InvalidEpochCandidate(curIndex, invalidHeights, error)) -> newP
          case Left(candidate) =>
            NewEpochFailed(candidate) -> copy(invalidIndexes = invalidIndexes + candidate.epochIndex)
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
        BlockHttpClient
          .buildBlock(bestBlock, blockCache.byId.get(bestBlock.header.parentId).map(_.stats))
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

    def updateEpochIndexes(persistedEpochIndexes: TreeMap[Int, BlockInfo]): ProgressState = {
      val newEpochIndexes = lastBlockIdInEpoch ++ persistedEpochIndexes.mapValues(_.stats.headerId)
      val newBlockCache =
        BlockCache(
          blockCache.byId ++ persistedEpochIndexes.map(i => i._2.stats.headerId -> i._2),
          blockCache.byHeight ++ persistedEpochIndexes.map(i => i._2.stats.height -> i._2)
        )
      ProgressState(newEpochIndexes, invalidIndexes, newBlockCache)
    }

    def epochIndexesToDownload(syncedNodeHeight: Int): SortedSet[Int] = {
      val startWithEpoch = persistedEpochIndexes.lastOption.map(_ + 1).getOrElse(Const.PreGenesisHeight)
      val endWithEpoch   = Epoch.epochIndexForHeight(syncedNodeHeight) - 1
      findMissingIndexes ++ TreeSet((startWithEpoch to endWithEpoch): _*)
    }

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

      s"Persisted Epochs: ${existingEpochs.size}${headStr(existingEpochs)}${lastStr(existingEpochs)}, " +
      s"Blocks cache size (heights): ${cachedHeights.size}${headStr(cachedHeights)}${lastStr(cachedHeights)}, " +
      s"Invalid Epochs: ${invalidIndexes.size}${headStr(invalidIndexes)}${lastStr(invalidIndexes)}"
    }

  }

}
