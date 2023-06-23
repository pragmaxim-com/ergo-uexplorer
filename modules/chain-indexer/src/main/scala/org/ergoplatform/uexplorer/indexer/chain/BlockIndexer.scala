package org.ergoplatform.uexplorer.indexer.chain

import akka.{Done, NotUsed}
import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
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
import org.ergoplatform.uexplorer.cassandra.api.Backend
import org.ergoplatform.uexplorer.chain.ChainTip
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ApiTransaction}
import org.ergoplatform.uexplorer.storage.{MvStorage, MvStoreConf}
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
  mvStoreConf: MvStoreConf
)(implicit enc: ErgoAddressEncoder)
  extends LazyLogging {

  def finishIndexing: Try[Unit] = {
    storage.writeReport.recover { case ex =>
      logger.error("Failed to generate report", ex)
    }
    storage
      .compact(indexing = false, mvStoreConf.maxIndexingCompactTime, mvStoreConf.maxIdleCompactTime)
      .recover { case ex =>
        logger.error("Compaction failed", ex)
      }
  }

  def readableStorage: Storage = storage

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

  private def hasParentAndIsChained(fork: List[LinkedBlock]): Boolean =
    fork.size > 1 && storage.containsBlock(fork.head.info.parentId, fork.head.info.height - 1) &&
      fork.sliding(2).forall {
        case first :: second :: Nil =>
          first.b.header.id == second.info.parentId
        case _ =>
          false
      }

  def rollbackFork(winningFork: List[LinkedBlock], backendOpt: Option[Backend]): Try[List[LinkedBlock]] =
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
        toRemove = winningFork.flatMap(b => storage.getBlocksByHeight(b.info.height).filter(_._1 != b.b.header.id)).toMap
        _ <- storage.rollbackTo(preForkVersion)
        _ <- backendOpt.fold(Success(()))(_.removeBlocksFromMainChain(toRemove.keys))
      } yield winningFork
    }

  val insertBlockFlow: Flow[LinkedBlock, BestBlockInserted, NotUsed] =
    Flow
      .apply[LinkedBlock]
      .map(utxoTracker.getBlockWithInputs(_).get)
      .async
      .wireTap(b => storage.persistErgoTreeUtxos(b.outputRecords).get)
      .wireTap(b => storage.removeInputBoxesByErgoTree(b.inputRecords).get)
      .async
      .wireTap(b => storage.persistErgoTreeTemplateUtxos(b.outputRecords).get)
      .wireTap(b => storage.removeInputBoxesByErgoTreeT8(b.inputRecords).get)
      .wireTap(b => storage.commitNewBlock(b.b.header.id, b.info, mvStoreConf, storage.getCurrentRevision).get)
      .async
      .map(lb => BestBlockInserted(lb, None))

}

object BlockIndexer {
  def apply(
    storage: MvStorage,
    utxoTracker: UtxoTracker,
    mvStoreConf: MvStoreConf
  )(implicit
    system: ActorSystem[Nothing],
    enc: ErgoAddressEncoder
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
