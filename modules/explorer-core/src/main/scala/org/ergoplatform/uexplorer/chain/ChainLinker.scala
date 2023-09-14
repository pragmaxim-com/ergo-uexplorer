package org.ergoplatform.uexplorer.chain

import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.node.ApiFullBlock
import zio.*

import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Try}

class ChainTip(byBlockIdRef: Ref[FifoLinkedHashMap[BlockId, Block]]) {
  def latestBlock: Task[Option[Block]] = toMap.map(_.values.toSeq.sortBy(_.height).lastOption)
  def toMap: Task[Map[BlockId, Block]] = byBlockIdRef.get.map(_.asScala.toMap)
  def getParent(block: ApiFullBlock): Task[Option[Block]] =
    byBlockIdRef.get
      .map(byBlockId => Option(byBlockId.get(block.header.parentId)))
      .flatMap {
        case Some(parent) if parent.height == block.header.height - 1 =>
          ZIO.succeed(Some(parent))
        case None =>
          ZIO.succeed(None)
        case Some(parent) =>
          ZIO.fail(illEx(s"Unexpected parent block ${parent.height} @ ${parent.blockId} of child ${block.header.height} @ ${block.header.id}"))
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
    newFifoMap.putAll(chainTip.iterator.toMap.asJava)
    Ref.make(newFifoMap).map(new ChainTip(_))
  }

  def empty: Task[ChainTip] = ChainTip.fromIterable(List.empty)

}

class ChainLinker(getBlock: BlockId => Task[ApiFullBlock], chainTip: ChainTip)(implicit ps: CoreConf) {

  def linkChildToAncestors(acc: List[BlockWithOutputs] = List.empty)(
    block: BlockWithOutputs
  ): Task[List[LinkedBlock]] =
    chainTip.getParent(block.b).flatMap {
      case parentBlockOpt if parentBlockOpt.isDefined || block.b.header.height == 1 =>
        val newBlock = block.toLinkedBlock(BlockBuilder(block, parentBlockOpt), parentBlockOpt)
        chainTip.putOnlyNew(newBlock.block) *>
        ZIO
          .foldLeft(acc)(List(newBlock)) { case (linkedBlocks, b) =>
            val parent = linkedBlocks.head.parentBlockOpt
            chainTip
              .putOnlyNew(BlockBuilder(b, parent))
              .map(newBlock => b.toLinkedBlock(newBlock, parent) :: linkedBlocks)
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
