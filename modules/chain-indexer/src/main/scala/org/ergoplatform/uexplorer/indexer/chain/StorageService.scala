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
import org.ergoplatform.uexplorer.mvstore.MaxCompactTime
import org.ergoplatform.uexplorer.mvstore.MultiMapLike.MultiMapSize
import org.ergoplatform.uexplorer.mvstore.SuperNodeCollector.Counter
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

class StorageService(
  storage: MvStorage,
  mvStoreConf: MvStoreConf
) extends LazyLogging {

  private def clearEmptySuperNodes(): Try[Unit] =
    storage.utxosByErgoTreeHex.clearEmptySuperNodes().flatMap { _ =>
      storage.utxosByErgoTreeT8Hex.clearEmptySuperNodes()
    }

  def compact(
    indexing: Boolean,
    maxIndexingCompactTime: MaxCompactTime,
    maxIdleCompactTime: MaxCompactTime
  ): Try[Unit] = Try {
    logger.info(s"Compacting file at $getCompactReport")
    val compactTime = if (indexing) maxIndexingCompactTime else maxIdleCompactTime
    val result      = if (!indexing) clearEmptySuperNodes() else Success(())
    result.map(_ => storage.store.compactFile(compactTime.toMillis.toInt))
  }

  private def getCompactReport: String = {
    val height                                                      = storage.getLastHeight.getOrElse(0)
    val MultiMapSize(superNodeSize, superNodeTotalSize, commonSize) = storage.utxosByErgoTreeHex.size
    val nonEmptyAddressCount                                        = superNodeSize + commonSize
    val progress =
      s"storage height: $height, " +
        s"utxo count: ${storage.ergoTreeHexByUtxo.size}, " +
        s"supernode-utxo-count : $superNodeTotalSize, " +
        s"non-empty-address count: $nonEmptyAddressCount, "
    val store = storage.store
    val cs    = store.getCacheSize
    val csu   = store.getCacheSizeUsed
    val chr   = store.getCacheHitRatio
    val cc    = store.getChunkCount
    val cfr   = store.getChunksFillRate
    val fr    = store.getFillRate
    val lr    = store.getLeafRatio
    val pc    = store.getPageCount
    val mps   = store.getMaxPageSize
    val kpp   = store.getKeysPerPage
    val debug =
      s"cache size used: $csu from: $cs at ratio: $chr, chunks: $cc at fill rate: $cfr, fill rate: $fr, " +
        s"leaf ratio: $lr, page count: $pc, max page size: $mps, keys per page: $kpp"
    progress + debug
  }

  def writeReportAndCompact(blocksIndexed: Int): Try[Unit] =
    if (blocksIndexed > 100000) {
      storage.getReportByPath.foreach { case (path, report) =>
        val lines =
          report.map { case (hotKey, Counter(writeOps, readOps, boxesAdded, boxesRemoved)) =>
            val stats  = s"$writeOps $readOps $boxesAdded $boxesRemoved ${boxesAdded - boxesRemoved}"
            val indent = 45
            s"$stats ${List.fill(Math.max(4, indent - stats.length))(" ").mkString("")} $hotKey"
          }
        tool.FileUtils.writeReport(lines, path).recover { case ex =>
          logger.error(s"Failing to write report", ex)
        }
      }
      compact(true, mvStoreConf.maxIndexingCompactTime, mvStoreConf.maxIdleCompactTime)
        .recover { case ex =>
          logger.error("Compaction failed", ex)
        }
    } else Success(())

  def readableStorage: Storage = storage

}

object StorageService {
  def apply(
    storage: MvStorage,
    mvStoreConf: MvStoreConf
  )(implicit
    system: ActorSystem[Nothing]
  ): StorageService = {
    CoordinatedShutdown(system).addTask(
      CoordinatedShutdown.PhaseServiceStop,
      "close-mv-store"
    ) { () =>
      Future(storage.store.close()).map(_ => Done)
    }
    new StorageService(storage, mvStoreConf)
  }
}
