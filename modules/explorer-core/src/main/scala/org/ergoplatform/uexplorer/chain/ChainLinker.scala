package org.ergoplatform.uexplorer.chain

import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.chain.ChainTip.FifoLinkedHashMap
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.node.ApiFullBlock
import zio.*

import java.util
import scala.jdk.CollectionConverters.*

class ChainTip(byBlockId: FifoLinkedHashMap[BlockId, Block]) {
  def latestBlock: Option[Block] = toMap.values.toSeq.sortBy(_.height).lastOption
  def toMap: Map[BlockId, Block] = byBlockId.asScala.toMap
  def getParent(block: ApiFullBlock): Option[Block] =
    Option(byBlockId.get(block.header.parentId)).filter(_.height == block.header.height - 1)
  def putOnlyNew(blockId: BlockId, block: Block): Task[Block] =
    Option(byBlockId.put(blockId, block)).fold(ZIO.succeed(block)) { oldVal =>
      ZIO.fail(
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

  def linkChildToAncestors(acc: List[LinkedBlock] = List.empty)(
    block: BlockWithOutputs
  ): Task[List[LinkedBlock]] =
    chainTip.getParent(block.b) match {
      case parentBlockOpt if parentBlockOpt.isDefined || block.b.header.height == 1 =>
        chainTip.putOnlyNew(block.b.header.id, BlockBuilder(block, parentBlockOpt)).map { newBlock =>
          block.toLinkedBlock(newBlock, parentBlockOpt) :: acc
        }
      case _ =>
        for {
          _            <- ZIO.log(s"Fork detected at height ${block.b.header.height} and block ${block.b.header.id}")
          apiBlock     <- getBlock(block.b.header.parentId)
          rewardBlock  <- RewardCalculator(apiBlock)
          outputBlock  <- OutputBuilder(rewardBlock)(ps.addressEncoder)
          linkedBlocks <- linkChildToAncestors(acc)(outputBlock)
        } yield linkedBlocks
    }
}
