package org.ergoplatform.uexplorer.chain

import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.node.ApiFullBlock
import zio.*

import scala.jdk.CollectionConverters.*

class ChainTip(byBlockIdRef: Ref[FifoLinkedHashMap[BlockId, Block]]) {
  def latestBlock: Task[Option[Block]] = toMap.map(_.values.toSeq.sortBy(_.height).lastOption)
  def toMap: Task[Map[BlockId, Block]] = byBlockIdRef.get.map(_.asScala.toMap)
  def getParent(block: ApiFullBlock): Task[Option[Block]] =
    byBlockIdRef.get
      .map { byBlockId =>
        byBlockId.asScala.values.toSeq.sortBy(_.height).lastOption -> Option(byBlockId.get(block.header.parentId))
      }
      .flatMap {
        case (_, Some(parent)) if parent.height == block.header.height - 1 =>
          ZIO.some(parent)
        case (_, None) if block.header.height == 1 =>
          ZIO.none
        case (lastBlockOpt, None) =>
          val lastBlockStr = lastBlockOpt.map(b => s"${b.height} @ ${b.blockId}")
          ZIO.logWarning(s"Parent not found, last cached block : $lastBlockStr").as(None)
        case (lastBlockOpt, Some(parent)) =>
          val lastBlockStr   = lastBlockOpt.map(b => s"${b.height} @ ${b.blockId}")
          val parentBlockStr = s"${parent.height} @ ${parent.blockId}"
          val childBlockStr  = s"${block.header.height} @ ${block.header.id}"
          ZIO.fail(illEx(s"Unexpected error, last cachedBlock $lastBlockStr, parent block $parentBlockStr of child $childBlockStr"))
      }

  def putOnlyNew(block: Block): Task[Block] =
    byBlockIdRef
      .modify { byBlockId =>
        Option(byBlockId.put(block.blockId, block)) -> byBlockId
      }
      .flatMap {
        case None =>
          ZIO.succeed(block)
        case Some(oldBlock) =>
          ZIO.fail(illEx(s"Trying to cache blockId ${block.blockId} at height ${block.height} but there already was $oldBlock"))
      }
}

object ChainTip {
  def fromIterable(chainTip: IterableOnce[(BlockId, Block)], maxSize: Int = 100): Task[ChainTip] = {
    val newFifoMap = new FifoLinkedHashMap[BlockId, Block](maxSize)
    newFifoMap.putAll(chainTip.iterator.toSeq.sortBy(_._2.height).toMap.asJava)
    Ref.make(newFifoMap).map(new ChainTip(_))
  }

  def empty: Task[ChainTip] = ChainTip.fromIterable(List.empty)

}

case class ChainLinker(getBlock: BlockId => Task[ApiFullBlock], chainTip: ChainTip)(implicit ps: CoreConf) {

  def linkChildToAncestors(acc: List[BlockWithOutputs] = List.empty)(
    block: BlockWithOutputs
  ): Task[List[LinkedBlock]] =
    chainTip.getParent(block.b).flatMap {
      case parentBlockOpt if parentBlockOpt.isDefined || block.b.header.height == 1 =>
        val newBlock = block.toLinkedBlock(BlockBuilder(block, parentBlockOpt), parentBlockOpt)
        chainTip.putOnlyNew(newBlock.block) *>
        ZIO
          .foldLeft(acc)(List(newBlock)) { case (linkedBlocks, b) =>
            val parent = linkedBlocks.headOption.map(_.block)
            chainTip
              .putOnlyNew(BlockBuilder(b, parent))
              .map(newB => b.toLinkedBlock(newB, parent) :: linkedBlocks)
          }
          .map(_.reverse)
      case _ =>
        for {
          _                 <- ZIO.log(s"Fork detected ${block.b.header.height} @ ${block.b.header.id} -> ${block.b.header.parentId}")
          parentApiBlock    <- getBlock(block.b.header.parentId)
          parentRewardBlock <- RewardCalculator(parentApiBlock)
          parentOutputBlock <- OutputBuilder(parentRewardBlock)(ps.addressEncoder)
          linkedBlocks      <- linkChildToAncestors(block :: acc)(parentOutputBlock)
        } yield linkedBlocks
    }
}
