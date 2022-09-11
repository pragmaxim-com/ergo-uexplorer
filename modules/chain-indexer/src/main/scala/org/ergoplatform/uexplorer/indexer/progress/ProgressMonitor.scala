package org.ergoplatform.uexplorer.indexer.progress

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import cats.Applicative
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.BlockId
import org.ergoplatform.explorer.BuildFrom.syntax._
import org.ergoplatform.explorer.db.models.{BlockStats, Header}
import org.ergoplatform.explorer.indexer.extractors._
import org.ergoplatform.explorer.indexer.models.{FlatBlock, SlotData}
import org.ergoplatform.explorer.protocol.models.ApiFullBlock
import org.ergoplatform.explorer.settings.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.Const
import org.ergoplatform.uexplorer.indexer.Resiliency.StopException
import tofu.Context

import scala.collection.immutable.{SortedMap, SortedSet, TreeMap, TreeSet}
import scala.collection.mutable.ListBuffer
import scala.util.Try

class ProgressMonitor(protocol: ProtocolSettings) extends LazyLogging {
  import ProgressMonitor._

  def initialBehavior: Behavior[ProgressMonitorRequest] =
    Behaviors.setup[ProgressMonitorRequest] { _ =>
      logger.info(s"Starting epoch monitoring ...")
      initialized(Progress(TreeMap.empty, TreeSet.empty, BlockCache(Map.empty, TreeMap.empty)))
    }

  def initialized(p: Progress): Behaviors.Receive[ProgressMonitorRequest] =
    Behaviors.receiveMessage[ProgressMonitorRequest] {
      case InsertBestBlock(bestBlock, replyTo) =>
        val flatBlock = buildBlock(SlotData(bestBlock, p.blockCache.byId.get(bestBlock.header.id).map(_.stats)), protocol)
        replyTo ! BestBlockInserted(flatBlock)
        val newBlockInfo = BlockInfo(flatBlock.header.parentId, flatBlock.info)
        initialized(
          p.copy(blockCache =
            BlockCache(
              p.blockCache.byId.updated(flatBlock.header.id, newBlockInfo),
              p.blockCache.byHeight.updated(flatBlock.header.height, newBlockInfo)
            )
          )
        )
      case InsertWinningFork(apiBlocks, replyTo) =>
        val (newBlocks, supersededBlocks) =
          apiBlocks.foldLeft(ListBuffer.empty[FlatBlock], ListBuffer.empty[BlockInfo]) {
            case ((newBlocksAcc, toRemoveAcc), apiBlock) =>
              val newBlocks =
                newBlocksAcc :+ buildBlock(
                  SlotData(apiBlock, p.blockCache.byId.get(apiBlock.header.id).map(_.stats)),
                  protocol
                )
              val toRemove =
                toRemoveAcc ++ p.blockCache.byHeight
                  .get(apiBlock.header.height)
                  .filter(_.stats.headerId != apiBlock.header.id)
              newBlocks -> toRemove
          }
        val newBlockCache =
          BlockCache(
            p.blockCache.byId ++ newBlocks.map(b => b.header.id -> BlockInfo(b.header.parentId, b.info)),
            p.blockCache.byHeight ++ newBlocks.map(b => b.header.height -> BlockInfo(b.header.parentId, b.info))
          )
        replyTo ! WinningForkInserted(newBlocks.toList, supersededBlocks.toList)
        initialized(p.copy(blockCache = newBlockCache))
      case GetLastBlock(replyTo) =>
        replyTo ! CachedBlock(p.blockCache.byHeight.lastOption.map(_._2))
        Behaviors.same
      case GetBlock(blockId, replyTo) =>
        replyTo ! CachedBlock(p.blockCache.byId.get(blockId))
        Behaviors.same
      case UpdateEpochIndexes(currentEpochIndexes, replyTo) =>
        val newEpochIndexes = p.lastBlockIdInEpoch ++ currentEpochIndexes.mapValues(_.stats.headerId)
        val newBlockCache =
          BlockCache(
            p.blockCache.byId ++ currentEpochIndexes.lastOption.map(i => i._2.stats.headerId -> i._2),
            p.blockCache.byHeight ++ currentEpochIndexes.lastOption.map(i => i._2.stats.height -> i._2)
          )
        val newProgress = Progress(newEpochIndexes, p.invalidIndexes, newBlockCache)
        logger.info(s"$newProgress")
        replyTo ! newProgress
        initialized(newProgress)
      case GetChainState(replyTo) =>
        replyTo ! p
        Behaviors.same
      case BlockPersisted(nextEpochBlock, replyTo) if atMaxForkPoint(nextEpochBlock) =>
        def getEpochCandidate(epochIndex: Int) =
          EpochCandidate(
            Epoch
              .heightRangeForEpochIndex(epochIndex)
              .map { height =>
                val info = p.blockCache.byHeight(height)
                height -> BlockRel(info.stats.headerId, info.parentId)
              }
          )

        def removeEpochFromCache(heightsToRemove: Seq[Int]): BlockCache =
          BlockCache(
            p.blockCache.byId -- heightsToRemove.flatMap(p.blockCache.byHeight.get).map(_.stats.headerId),
            p.blockCache.byHeight -- heightsToRemove
          )

        val currentEpochIndex  = Epoch.epochIndexForHeight(nextEpochBlock.header.height) - 1
        val previousEpochIndex = currentEpochIndex - 1

        if (p.lastBlockIdInEpoch.contains(currentEpochIndex)) {
          replyTo ! NewEpochExisted(currentEpochIndex)
          Behaviors.same
        } else {
          getEpochCandidate(currentEpochIndex) match {
            case Right(candidate) if currentEpochIndex == 0 =>
              val newEpoch = candidate.getEpoch
              replyTo ! NewEpochCreated(newEpoch)
              val newP = p.copy(lastBlockIdInEpoch = p.lastBlockIdInEpoch.updated(newEpoch.index, newEpoch.blockIds.last))
              logger.info(s"$newP")
              initialized(newP)
            case Right(candidate) if p.lastBlockIdInEpoch(previousEpochIndex) == candidate.relsByHeight.head._2.parentId =>
              val newBlockCache = removeEpochFromCache(Epoch.heightRangeForEpochIndex(previousEpochIndex))
              val newEpoch      = candidate.getEpoch
              replyTo ! NewEpochCreated(newEpoch)
              val newP =
                Progress(
                  p.lastBlockIdInEpoch.updated(newEpoch.index, newEpoch.blockIds.last),
                  p.invalidIndexes,
                  newBlockCache
                )
              logger.info(s"$newP")
              initialized(newP)
            case Right(candidate) =>
              val Epoch(curIndex, curHeaders) = candidate.getEpoch
              val error =
                s"Prev epoch $previousEpochIndex header ${p.lastBlockIdInEpoch.get(previousEpochIndex)} " +
                s"does not match current epoch $curIndex header ${curHeaders.head}"
              val invalidHeights =
                TreeSet(Epoch.heightRangeForEpochIndex(previousEpochIndex).last, candidate.relsByHeight.head._1)
              replyTo ! NewEpochFailed(
                InvalidEpochCandidate(curIndex, invalidHeights, error)
              )
              initialized(p.copy(invalidIndexes = p.invalidIndexes + candidate.epochIndex))
            case Left(candidate) =>
              replyTo ! NewEpochFailed(candidate)
              initialized(p.copy(invalidIndexes = p.invalidIndexes + candidate.epochIndex))
          }
        }
      case BlockPersisted(_, _) =>
        logger.error(s"Received unexpected block, stopping...")
        Behaviors.stopped
    }
}

object ProgressMonitor {

  case class BlockInfo(parentId: BlockId, stats: BlockStats)

  sealed trait ProgressMonitorRequest

  sealed trait Insertable extends ProgressMonitorRequest {
    def replyTo: ActorRef[Inserted]
  }

  case class InsertBestBlock(block: ApiFullBlock, replyTo: ActorRef[Inserted]) extends Insertable

  case class InsertWinningFork(blocks: List[ApiFullBlock], replyTo: ActorRef[Inserted]) extends Insertable

  case class GetBlock(blockId: BlockId, replyTo: ActorRef[CachedBlock]) extends ProgressMonitorRequest

  case class GetLastBlock(replyTo: ActorRef[CachedBlock]) extends ProgressMonitorRequest

  case class UpdateEpochIndexes(lastBlockIdByEpochIndex: TreeMap[Int, BlockInfo], replyTo: ActorRef[Progress])
    extends ProgressMonitorRequest

  case class GetChainState(replyTo: ActorRef[Progress]) extends ProgressMonitorRequest

  case class BlockPersisted(block: FlatBlock, replyTo: ActorRef[MaybeNewEpoch]) extends ProgressMonitorRequest

  sealed trait ProgressMonitorResponse

  case class CachedBlock(block: Option[BlockInfo]) extends ProgressMonitorResponse

  sealed trait Inserted extends ProgressMonitorResponse

  case class BestBlockInserted(flatBlock: FlatBlock) extends Inserted

  case class WinningForkInserted(newFork: List[FlatBlock], supersededFork: List[BlockInfo]) extends Inserted

  sealed trait MaybeNewEpoch extends ProgressMonitorResponse

  case class NewEpochCreated(epochCandidate: Epoch) extends MaybeNewEpoch

  case class NewEpochFailed(epochCandidate: InvalidEpochCandidate) extends MaybeNewEpoch

  case class NewEpochExisted(epochIndex: Int) extends MaybeNewEpoch

  case class BlockCache(
    byId: Map[BlockId, BlockInfo],
    byHeight: SortedMap[Int, BlockInfo]
  ) {
    def heights: SortedSet[Int] = byHeight.keySet
  }

  case class Progress(
    lastBlockIdInEpoch: TreeMap[Int, BlockId],
    invalidIndexes: TreeSet[Int],
    blockCache: BlockCache
  ) extends ProgressMonitorResponse {

    override def toString: String = {
      val existingEpochs = epochIndexes
      val cachedHeights  = blockCache.heights

      def headStr(xs: SortedSet[Int]) = xs.headOption.map(h => s"[$h").getOrElse("")

      def lastStr(xs: SortedSet[Int]) =
        if (xs.isEmpty) ""
        else xs.lastOption.filterNot(xs.headOption.contains).map(h => s" - $h]").getOrElse("]")

      s"Persisted Epochs: ${existingEpochs.size}${headStr(existingEpochs)}${lastStr(existingEpochs)}, " +
      s"Blocks cache size (heights): ${cachedHeights.size}${headStr(cachedHeights)}${lastStr(cachedHeights)}, " +
      s"Invalid Epochs: ${invalidIndexes.size}${headStr(invalidIndexes)}${lastStr(invalidIndexes)}"
    }

    def epochIndexesToDownload(syncedNodeHeight: Int): SortedSet[Int] = {
      val startWithEpoch = epochIndexes.lastOption.map(_ + 1).getOrElse(Const.PreGenesisHeight)
      val endWithEpoch   = Epoch.epochIndexForHeight(syncedNodeHeight) - 1
      findMissingIndexes ++ TreeSet((startWithEpoch to endWithEpoch): _*)
    }

    def epochIndexes: SortedSet[Int] = lastBlockIdInEpoch.keySet

    def findMissingIndexes: TreeSet[Int] =
      if (lastBlockIdInEpoch.isEmpty || lastBlockIdInEpoch.size == 1)
        TreeSet.empty
      else
        TreeSet((lastBlockIdInEpoch.head._1 to lastBlockIdInEpoch.last._1): _*)
          .diff(lastBlockIdInEpoch.keySet)

  }

  def atMaxForkPoint(block: FlatBlock): Boolean =
    Epoch.epochIndexForHeight(block.header.height) > 0 && block.header.height % Const.EpochLength == Const.FlushHeight

  def updateMainChain(block: FlatBlock, mainChain: Boolean): FlatBlock = {
    import monocle.macros.GenLens
    val header          = GenLens[FlatBlock](_.header)
    val headerMainChain = GenLens[Header](_.mainChain)
    val info            = GenLens[FlatBlock](_.info)
    val infoMainChain   = GenLens[BlockStats](_.mainChain)
    val txs             = GenLens[FlatBlock](_.txs)
    val inputs          = GenLens[FlatBlock](_.inputs)
    val dataInputs      = GenLens[FlatBlock](_.dataInputs)
    val outputs         = GenLens[FlatBlock](_.outputs)

    (header composeLens headerMainChain).modify(_ => mainChain)(
      (info composeLens infoMainChain).modify(_ => mainChain)(
        txs.modify(_.map(_.copy(mainChain = mainChain)))(
          inputs.modify(_.map(_.copy(mainChain = mainChain)))(
            dataInputs.modify(_.map(_.copy(mainChain = mainChain)))(
              outputs.modify(_.map(_.copy(mainChain = mainChain)))(
                block
              )
            )
          )
        )
      )
    )
  }

  def buildBlock(slotData: SlotData, protocol: ProtocolSettings): FlatBlock = {
    implicit val ctx = Context.const(protocol)(Applicative[Try])
    slotData
      .intoF[Try, FlatBlock]
      .map(updateMainChain(_, mainChain = true))
      .getOrElse(throw new StopException(s"Block $slotData is invalid", null))
  }

}
