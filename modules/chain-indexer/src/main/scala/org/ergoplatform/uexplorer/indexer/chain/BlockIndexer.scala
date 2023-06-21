package org.ergoplatform.uexplorer.indexer.chain

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.node.ApiFullBlock

import scala.collection.compat.immutable.ArraySeq
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferInput, ByteBufferOutput, Input, Output}
import com.esotericsoftware.kryo.serializers.MapSerializer
import com.esotericsoftware.kryo.util.Pool
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.shaded.kryo.pool.KryoPool
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.Const.Protocol.{Emission, Foundation}
import org.ergoplatform.uexplorer.indexer.chain.ChainIndexer.ChainTip
import org.ergoplatform.uexplorer.indexer.config.MvStore
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ApiTransaction}
import org.ergoplatform.uexplorer.storage.MvStorage
import org.ergoplatform.uexplorer.storage.MvStorage.*
import org.h2.mvstore.{MVMap, MVStore}

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util
import java.util.concurrent.ConcurrentSkipListMap
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.{ListMap, TreeMap, TreeSet}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Random, Success, Try}

class BlockIndexer(
  storage: MvStorage,
  mvStoreConf: MvStore
) extends LazyLogging {

  def readableStorage: Storage = storage

  def addBestBlocks(winningFork: List[LinkedBlock]): Try[ListBuffer[BestBlockInserted]] =
    winningFork
      .foldLeft(Try(ListBuffer.empty[BestBlockInserted])) {
        case (f @ Failure(_), _) =>
          f
        case (Success(insertedBlocksAcc), apiBlock) =>
          addBestBlock(apiBlock).map { insertedBlock =>
            insertedBlocksAcc :+ insertedBlock
          }
      }

  def compact(indexing: Boolean): Try[Unit] = Try {
    logger.info(s"Compacting file at ${storage.getCompactReport}")
    val compactTime = if (indexing) mvStoreConf.maxIndexingCompactTime else mvStoreConf.maxIdleCompactTime
    val result      = if (!indexing) storage.clear() else Success(())
    result.map(_ => storage.store.compactFile(compactTime.toMillis.toInt))
  }

  def getChainTip: Try[ChainTip] = Try {
    val chainTip =
      ChainTip(
        ListMap.from(
          storage.blockIdsByHeight
            .iterator(None, None, reverse = true)
            .take(100)
            .flatMap { case (_, blockIds) =>
              blockIds.asScala.flatMap(b => storage.blockById.get(b).map(b -> _))
            }
        )
      )
    assert(chainTip.lastHeight == storage.getLastHeight, "MvStore's Iterator works unexpectedly!")
    chainTip
  }

  def addBestBlock(block: LinkedBlock): Try[BestBlockInserted] =
    for {
      lb <- UtxoTracker(block, storage.getAddressByUtxo, storage.getUtxoValueByAddress)
      _  <- storage.persistNewBlock(lb)
      _  <- if (lb.blockInfo.height % mvStoreConf.heightCompactRate == 0) compact(true) else Success(())
    } yield BestBlockInserted(lb, None) // TODO we forgot about FullBlock !

  private def hasParentAndIsChained(fork: List[LinkedBlock]): Boolean =
    fork.size > 1 && storage.containsBlock(fork.head.blockInfo.parentId, fork.head.blockInfo.height - 1) &&
      fork.sliding(2).forall {
        case first :: second :: Nil =>
          first.block.header.id == second.blockInfo.parentId
        case _ =>
          false
      }

  def addWinningFork(winningFork: List[LinkedBlock]): Try[ForkInserted] =
    if (!hasParentAndIsChained(winningFork)) {
      Failure(
        new UnexpectedStateError(
          s"Inserting fork ${winningFork.map(_.block.header.id).mkString(",")} at height ${winningFork.map(_.blockInfo.height).mkString(",")} illegal"
        )
      )
    } else {
      logger.info(s"Adding fork from height ${winningFork.head.blockInfo.height} until ${winningFork.last.blockInfo.height}")
      for {
        preForkVersion <- Try(storage.getBlockById(winningFork.head.block.header.id).map(_.revision).get)
        toRemove =
          winningFork.flatMap(b => storage.getBlocksByHeight(b.blockInfo.height).filter(_._1 != b.block.header.id)).toMap
        _         <- storage.rollbackTo(preForkVersion)
        newBlocks <- addBestBlocks(winningFork)
      } yield ForkInserted(
        newBlocks.toList,
        toRemove
      )
    }
}

object BlockIndexer {
  def apply(
    storage: MvStorage,
    mvStoreConf: MvStore
  )(implicit
    system: ActorSystem[Nothing]
  ): BlockIndexer = {
    CoordinatedShutdown(system).addTask(
      CoordinatedShutdown.PhaseServiceStop,
      "close-mv-store"
    ) { () =>
      Future(storage.close()).map(_ => Done)
    }
    new BlockIndexer(storage, mvStoreConf)
  }
}
