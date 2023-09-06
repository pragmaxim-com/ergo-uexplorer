package org.ergoplatform.uexplorer.chain

import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.chain.ChainTip.FifoLinkedHashMap
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.node.ApiFullBlock
import zio.*

import java.util
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Try}

class ChainTip(byBlockId: FifoLinkedHashMap[BlockId, Block]) {
  def latestBlock: Option[Block] = toMap.values.toSeq.sortBy(_.height).lastOption
  def toMap: Map[BlockId, Block] = byBlockId.asScala.toMap
  def getParent(block: ApiFullBlock): Option[Block] =
    Option(byBlockId.get(block.header.parentId)).filter(_.height == block.header.height - 1)
  def putOnlyNew(blockId: BlockId, block: Block): Try[Block] =
    Option(byBlockId.put(blockId, block)).fold(Try(block)) { oldVal =>
      Failure(
        new AssertionError(
          s"Trying to cache blockId $blockId at height ${block.height} but there already was $oldVal"
        )
      )
    }

}
object ChainTip {
  def apply(chainTip: IterableOnce[(BlockId, Block)], maxSize: Int = 100): ChainTip = {
    val newFifoMap = new FifoLinkedHashMap[BlockId, Block](maxSize)
    newFifoMap.putAll(chainTip.iterator.toMap.asJava)
    new ChainTip(newFifoMap)
  }

  def empty: ChainTip = ChainTip(List.empty)

  class FifoLinkedHashMap[K, V](maxSize: Int = 100) extends util.LinkedHashMap[K, V] {
    override def removeEldestEntry(eldest: java.util.Map.Entry[K, V]): Boolean = this.size > maxSize
  }
}

class ChainLinker(getBlock: BlockId => Task[ApiFullBlock], chainTip: ChainTip)(implicit ps: CoreConf) {

  def linkChildToAncestors(acc: List[BlockWithOutputs] = List.empty)(
    block: BlockWithOutputs
  ): Task[List[LinkedBlock]] =
    chainTip.getParent(block.b) match {
      case parentBlockOpt if parentBlockOpt.isDefined || block.b.header.height == 1 =>
        ZIO.attempt {
          val newBlock = block.toLinkedBlock(BlockBuilder(block, parentBlockOpt), parentBlockOpt)
          chainTip.putOnlyNew(newBlock.b.header.id, newBlock.block)
          acc.foldLeft(List(newBlock)) { case (linkedBlocks, b) =>
            val parent = linkedBlocks.lastOption.flatMap(_.parentBlockOpt)
            chainTip
              .putOnlyNew(b.b.header.id, BlockBuilder(b, parent))
              .map(newBlock => linkedBlocks :+ b.toLinkedBlock(newBlock, parent))
              .get
          }
        }
      case _ =>
        for {
          _                 <- ZIO.log(s"Fork detected at height ${block.b.header.height} and block ${block.b.header.id}")
          parentApiBlock    <- getBlock(block.b.header.parentId)
          parentRewardBlock <- RewardCalculator(parentApiBlock)
          parentOutputBlock <- OutputBuilder(parentRewardBlock)(ps.addressEncoder)
          linkedBlocks      <- linkChildToAncestors(block :: acc)(parentOutputBlock)
        } yield linkedBlocks
    }
}
