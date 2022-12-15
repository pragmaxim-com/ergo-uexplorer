package org.ergoplatform.uexplorer.indexer.api

import akka.NotUsed
import akka.stream.scaladsl.Flow
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId}
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.chain.ChainSyncer.*
import org.ergoplatform.uexplorer.indexer.chain.{ChainState, Epoch}
import org.ergoplatform.uexplorer.indexer.chain.ChainState.BufferedBlockInfo
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState

import java.util.concurrent.ConcurrentHashMap
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

trait Backend {

  def blockWriteFlow: Flow[Inserted, Block, NotUsed]

  def epochsWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed]

  def loadUtxoState(epochIndexes: Iterator[Int]): Future[UtxoState]

  def loadBlockInfoByEpochIndex: Future[TreeMap[Int, BufferedBlockInfo]]
}

class InMemoryBackend extends Backend {

  private val lastBlockInfoByEpochIndex = new ConcurrentHashMap[Int, BufferedBlockInfo]()
  private val boxesByHeight             = new ConcurrentHashMap[Int, (ArraySeq[BoxId], ArraySeq[(BoxId, Address, Long)])]()
  private val blocksById                = new ConcurrentHashMap[BlockId, BufferedBlockInfo]()
  private val blocksByHeight            = new ConcurrentHashMap[Int, BufferedBlockInfo]()

  override def blockWriteFlow: Flow[Inserted, Block, NotUsed] =
    Flow[Inserted]
      .mapConcat {
        case BestBlockInserted(flatBlock) =>
          blocksByHeight.put(flatBlock.header.height, BufferedBlockInfo.fromBlock(flatBlock))
          boxesByHeight.put(
            flatBlock.header.height,
            (
              flatBlock.inputs.map(_.boxId),
              flatBlock.outputs.map(o => (o.boxId, o.address, o.value))
            )
          )
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
          lastBlockInfoByEpochIndex.put(
            epoch.index,
            blocksById.get(epoch.blockIds.last)
          )
          block -> Some(NewEpochCreated(epoch))
        case tuple =>
          tuple
      }

  override def loadUtxoState(epochIndexes: Iterator[Int]): Future[UtxoState] =
    Future.successful(UtxoState.empty.mergeBoxes(TreeMap.from(boxesByHeight.asScala).valuesIterator))

  def loadBlockInfoByEpochIndex: Future[TreeMap[Int, BufferedBlockInfo]] =
    Future.successful(TreeMap(lastBlockInfoByEpochIndex.asScala.toSeq: _*))

}
