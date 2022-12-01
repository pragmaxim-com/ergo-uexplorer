package org.ergoplatform.uexplorer.indexer.api

import akka.NotUsed
import akka.stream.scaladsl.Flow
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId}
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.chain.ChainSyncer.*
import org.ergoplatform.uexplorer.indexer.chain.{ChainState, Epoch, UtxoState}
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

  override def getCachedState: Future[ChainState] = {
    val state =
      boxesByHeight.asScala.foldLeft(UtxoState.empty) { case (stateAcc, (height, (inputs, outputs))) =>
        stateAcc.bufferBestBlock(height, inputs, outputs)
      }
    Future.successful(
      ChainState.load(
        TreeMap(lastBlockInfoByEpochIndex.asScala.toSeq: _*),
        state.mergeEpochFromBuffer(
          lastBlockInfoByEpochIndex.asScala.keysIterator.flatMap(Epoch.heightRangeForEpochIndex).toIndexedSeq
        )
      )
    )
  }

}
