package org.ergoplatform.uexplorer.indexer.api

import akka.NotUsed
import akka.stream.scaladsl.Flow
import org.ergoplatform.uexplorer.BlockId
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor._
import org.ergoplatform.uexplorer.indexer.progress.ProgressState.CachedBlock

import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable.TreeMap
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

trait Backend {

  def blockWriteFlow: Flow[Inserted, Block, NotUsed]

  def epochWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed]

  def getLastBlockInfoByEpochIndex: Future[TreeMap[Int, CachedBlock]]
}

class InMemoryBackend extends Backend {

  private val lastBlockInfoByEpochIndex = new ConcurrentHashMap[Int, CachedBlock]()
  private val blocksById                = new ConcurrentHashMap[BlockId, CachedBlock]()
  private val blocksByHeight            = new ConcurrentHashMap[Int, CachedBlock]()

  override def blockWriteFlow: Flow[Inserted, Block, NotUsed] =
    Flow[Inserted]
      .mapConcat {
        case BestBlockInserted(flatBlock) =>
          blocksByHeight.put(flatBlock.header.height, CachedBlock.fromBlock(flatBlock))
          blocksById.put(flatBlock.header.id, CachedBlock.fromBlock(flatBlock))
          List(flatBlock)
        case ForkInserted(winningFork, _) =>
          winningFork.foreach { flatBlock =>
            blocksByHeight.put(flatBlock.header.height, CachedBlock.fromBlock(flatBlock))
            blocksById.put(flatBlock.header.id, CachedBlock.fromBlock(flatBlock))
          }
          winningFork
      }

  override def epochWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed] =
    Flow[(Block, Option[MaybeNewEpoch])]
      .map {
        case (block, Some(NewEpochCreated(epoch))) =>
          lastBlockInfoByEpochIndex.put(epoch.index, blocksById.get(epoch.blockIds.last))
          block -> Some(NewEpochCreated(epoch))
        case tuple =>
          tuple
      }

  override def getLastBlockInfoByEpochIndex: Future[TreeMap[Int, CachedBlock]] =
    Future.successful(TreeMap(lastBlockInfoByEpochIndex.asScala.toSeq: _*))
}
