package org.ergoplatform.uexplorer.indexer.api

import akka.NotUsed
import akka.stream.scaladsl.Flow
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId}
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.chain.ChainSyncer.*
import org.ergoplatform.uexplorer.indexer.chain.{ChainState, UtxoState}
import org.ergoplatform.uexplorer.indexer.chain.ChainState.BufferedBlockInfo

import java.util.concurrent.ConcurrentHashMap
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

trait Backend {

  def blockWriteFlow: Flow[Inserted, Block, NotUsed]

  def epochsWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed]

  def getCachedState: Future[ChainState]
}

class InMemoryBackend extends Backend {

  private val lastBlockInfoByEpochIndex = new ConcurrentHashMap[Int, BufferedBlockInfo]()
  private val boxesByEpochIndex         = new ConcurrentHashMap[Int, (Iterable[BoxId], Map[Address, mutable.Map[BoxId, Long]])]()
  private val blocksById                = new ConcurrentHashMap[BlockId, BufferedBlockInfo]()
  private val blocksByHeight            = new ConcurrentHashMap[Int, BufferedBlockInfo]()

  override def blockWriteFlow: Flow[Inserted, Block, NotUsed] =
    Flow[Inserted]
      .mapConcat {
        case BestBlockInserted(flatBlock) =>
          blocksByHeight.put(flatBlock.header.height, BufferedBlockInfo.fromBlock(flatBlock))
          blocksById.put(flatBlock.header.id, BufferedBlockInfo.fromBlock(flatBlock))
          List(flatBlock)
        case ForkInserted(winningFork, _) =>
          winningFork.foreach { flatBlock =>
            blocksByHeight.put(flatBlock.header.height, BufferedBlockInfo.fromBlock(flatBlock))
            blocksById.put(flatBlock.header.id, BufferedBlockInfo.fromBlock(flatBlock))
          }
          winningFork
      }

  override def epochsWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed] =
    Flow[(Block, Option[MaybeNewEpoch])]
      .map {
        case (block, Some(NewEpochCreated(epoch))) =>
          boxesByEpochIndex.put(epoch.index, (epoch.inputIds, epoch.utxosByAddress))
          lastBlockInfoByEpochIndex.put(
            epoch.index,
            blocksById.get(epoch.blockIds.last)
          )
          block -> Some(NewEpochCreated(epoch))
        case tuple =>
          tuple
      }

  override def getCachedState: Future[ChainState] = {
    val (inputs, outputs) =
      boxesByEpochIndex.asScala.foldLeft((ArraySeq.empty[BoxId], Map.empty[Address, mutable.Map[BoxId, Long]])) {
        case ((iAcc, oAcc), (_, (i, o))) => (iAcc ++ i, oAcc ++ o)
      }
    Future.successful(
      ChainState.load(
        TreeMap(lastBlockInfoByEpochIndex.asScala.toSeq: _*),
        UtxoState.empty.mergeEpochFromBoxes(inputs, outputs)
      )
    )
  }

}
