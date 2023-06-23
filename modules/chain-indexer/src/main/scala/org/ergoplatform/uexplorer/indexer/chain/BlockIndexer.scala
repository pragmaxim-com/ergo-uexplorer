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
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.Const.Protocol.{Emission, Foundation}
import org.ergoplatform.uexplorer.chain.ChainTip
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
  utxoTracker: UtxoTracker,
  mvStoreConf: MvStore
) extends LazyLogging {

  def readableStorage: Storage = storage

  def addBestBlocks(winningFork: List[LinkedBlock])(implicit enc: ErgoAddressEncoder): Try[ListBuffer[BestBlockInserted]] =
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
        storage.blockIdsByHeight
          .iterator(None, None, reverse = true)
          .take(100)
          .flatMap { case (_, blockIds) =>
            blockIds.asScala.flatMap(b => storage.blockById.get(b).map(b -> _))
          }
      )
    val sortedKeys = chainTip.toMap.values.map(_.height).toSeq.sorted
    assert(
      sortedKeys.lastOption == storage.getLastHeight,
      s"MvStore's Iterator works unexpectedly, ${sortedKeys.mkString(", ")} but last key is ${storage.getLastHeight}!"
    )
    chainTip
  }

  def addBestBlock(block: LinkedBlock)(implicit enc: ErgoAddressEncoder): Try[BestBlockInserted] =
    for {
      lb <- utxoTracker.getBlockWithInputs(block)
      _  <- storage.persistNewBlock(lb)
      _  <- if (lb.info.height % mvStoreConf.heightCompactRate == 0) compact(true) else Success(())
    } yield BestBlockInserted(lb, None) // TODO we forgot about FullBlock !

  private def hasParentAndIsChained(fork: List[LinkedBlock]): Boolean =
    fork.size > 1 && storage.containsBlock(fork.head.info.parentId, fork.head.info.height - 1) &&
      fork.sliding(2).forall {
        case first :: second :: Nil =>
          first.b.header.id == second.info.parentId
        case _ =>
          false
      }

  def addWinningFork(winningFork: List[LinkedBlock])(implicit enc: ErgoAddressEncoder): Try[ForkInserted] =
    if (!hasParentAndIsChained(winningFork)) {
      Failure(
        new UnexpectedStateError(
          s"Inserting fork ${winningFork.map(_.b.header.id).mkString(",")} at height ${winningFork.map(_.info.height).mkString(",")} illegal"
        )
      )
    } else {
      logger.info(s"Adding fork from height ${winningFork.head.info.height} until ${winningFork.last.info.height}")
      for {
        preForkVersion <- Try(storage.getBlockById(winningFork.head.b.header.id).map(_.revision).get)
        toRemove =
          winningFork.flatMap(b => storage.getBlocksByHeight(b.info.height).filter(_._1 != b.b.header.id)).toMap
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
    utxoTracker: UtxoTracker,
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
    new BlockIndexer(storage, utxoTracker, mvStoreConf)
  }
}
