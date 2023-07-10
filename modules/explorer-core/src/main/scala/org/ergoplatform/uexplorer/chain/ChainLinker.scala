package org.ergoplatform.uexplorer.chain

import com.typesafe.scalalogging.LazyLogging
import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.chain.ChainTip.FifoLinkedHashMap
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ApiHeader}
import org.ergoplatform.{ErgoAddressEncoder, ErgoScriptPredef, Pay2SAddress}
import scorex.util.encode.Base16
import sigmastate.basics.DLogProtocol.ProveDlog
import sigmastate.serialization.{GroupElementSerializer, SigmaSerializer}

import java.util
import scala.collection.immutable.TreeSet
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

class ChainTip(byBlockId: FifoLinkedHashMap[BlockId, Block]) {
  def toMap: Map[BlockId, Block] = byBlockId.asScala.toMap
  def getParent(block: ApiFullBlock): Option[Block] =
    Option(byBlockId.get(block.header.parentId)).filter(_.height == block.header.height - 1)
  def putOnlyNew(blockId: BlockId, block: Block): Try[Block] =
    Option(byBlockId.put(blockId, block)).fold(Success(block)) { oldVal =>
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

  class FifoLinkedHashMap[K, V](maxSize: Int = 100) extends util.LinkedHashMap[K, V] {
    override def removeEldestEntry(eldest: java.util.Map.Entry[K, V]): Boolean = this.size > maxSize
  }
}

class ChainLinker(getBlock: BlockId => Future[ApiFullBlock], chainTip: ChainTip) extends LazyLogging {

  def linkChildToAncestors(acc: List[LinkedBlock] = List.empty)(
    block: BlockWithOutputs
  )(implicit ps: ProtocolSettings): Future[List[LinkedBlock]] =
    chainTip.getParent(block.b) match {
      case parentBlockOpt if parentBlockOpt.isDefined || block.b.header.height == 1 =>
        Future.fromTry(
          chainTip.putOnlyNew(block.b.header.id, BlockBuilder(block, parentBlockOpt)).map { newBlock =>
            block.toLinkedBlock(newBlock, parentBlockOpt) :: acc
          }
        )
      case _ =>
        logger.info(s"Encountered fork at height ${block.b.header.height} and block ${block.b.header.id}")
        for {
          apiBlock     <- getBlock(block.b.header.parentId)
          rewardBlock  <- Future.fromTry(RewardCalculator(apiBlock))
          outputBlock  <- Future.fromTry(OutputBuilder(rewardBlock)(ps.addressEncoder))
          linkedBlocks <- linkChildToAncestors(acc)(outputBlock)
        } yield linkedBlocks
    }
}
