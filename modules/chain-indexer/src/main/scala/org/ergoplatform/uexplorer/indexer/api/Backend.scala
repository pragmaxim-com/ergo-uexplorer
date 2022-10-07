package org.ergoplatform.uexplorer.indexer.api

import akka.NotUsed
import akka.stream.scaladsl.Flow
import org.ergoplatform.uexplorer.BlockId
import org.ergoplatform.uexplorer.db.{BlockInfo, FlatBlock}
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor._

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._
import scala.collection.immutable.TreeMap
import scala.concurrent.Future

trait Backend {

  def blockWriteFlow: Flow[Inserted, FlatBlock, NotUsed]

  def epochWriteFlow: Flow[(FlatBlock, Option[MaybeNewEpoch]), (FlatBlock, Option[MaybeNewEpoch]), NotUsed]

  def getLastBlockInfoByEpochIndex: Future[TreeMap[Int, BlockInfo]]
}

class InMemoryBackend extends Backend {

  private val lastBlockInfoByEpochIndex = new ConcurrentHashMap[Int, BlockInfo]()
  private val blocksById                = new ConcurrentHashMap[BlockId, BlockInfo]()
  private val blocksByHeight            = new ConcurrentHashMap[Int, BlockInfo]()

  override def blockWriteFlow: Flow[Inserted, FlatBlock, NotUsed] =
    Flow[Inserted]
      .mapConcat {
        case BestBlockInserted(flatBlock) =>
          blocksByHeight.put(flatBlock.header.height, flatBlock.info)
          blocksById.put(flatBlock.header.id, flatBlock.info)
          List(flatBlock)
        case ForkInserted(winningFork, _) =>
          winningFork.foreach { flatBlock =>
            blocksByHeight.put(flatBlock.header.height, flatBlock.info)
            blocksById.put(flatBlock.header.id, flatBlock.info)
          }
          winningFork
      }

  override def epochWriteFlow: Flow[(FlatBlock, Option[MaybeNewEpoch]), (FlatBlock, Option[MaybeNewEpoch]), NotUsed] =
    Flow[(FlatBlock, Option[MaybeNewEpoch])]
      .map {
        case (block, Some(NewEpochCreated(epoch))) =>
          lastBlockInfoByEpochIndex.put(epoch.index, blocksById.get(epoch.blockIds.last))
          block -> Some(NewEpochCreated(epoch))
        case tuple =>
          tuple
      }

  override def getLastBlockInfoByEpochIndex: Future[TreeMap[Int, BlockInfo]] =
    Future.successful(TreeMap(lastBlockInfoByEpochIndex.asScala.toSeq: _*))
}
