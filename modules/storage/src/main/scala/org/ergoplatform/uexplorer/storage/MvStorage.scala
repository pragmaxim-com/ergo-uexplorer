package org.ergoplatform.uexplorer.storage

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferInput, ByteBufferOutput, Input, Output}
import com.esotericsoftware.kryo.serializers.MapSerializer
import com.esotericsoftware.kryo.util.Pool
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.shaded.kryo.pool.KryoPool
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.Const.Protocol.{Emission, FeeContract, Foundation}
import org.ergoplatform.uexplorer.chain.ChainTip
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.mvstore.*
import org.ergoplatform.uexplorer.mvstore.multiset.MultiMvSet
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ApiTransaction}
import org.ergoplatform.uexplorer.storage.Implicits.*
import org.ergoplatform.uexplorer.storage.MvStorage.*
import org.h2.mvstore.{MVMap, MVStore}
import zio.*
import java.io.{BufferedInputStream, File}
import java.nio.ByteBuffer
import java.nio.file.{CopyOption, Files, Path, Paths}
import java.util
import java.util.Map.Entry
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap}
import java.util.stream.Collectors
import java.util.zip.GZIPInputStream
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.{TreeMap, TreeSet}
import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.io.Source
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Random, Success, Try}

case class MvStorage(
  utxosByErgoTreeHex: MultiMvSet[ErgoTreeHex, java.util.Set, BoxId],
  utxosByErgoTreeT8Hex: MultiMvSet[ErgoTreeT8Hex, java.util.Set, BoxId],
  ergoTreeHexByUtxo: MapLike[BoxId, ErgoTreeHex],
  blockIdsByHeight: MapLike[Height, java.util.Set[BlockId]],
  blockById: MapLike[BlockId, Block]
)(implicit val store: MVStore, mvStoreConf: MvStoreConf)
  extends WritableStorage
  with LazyLogging {

  private def getReportByPath: Map[Path, Vector[(String, SuperNodeCounter)]] =
    Map(
      utxosByErgoTreeHex.getReport,
      utxosByErgoTreeT8Hex.getReport
    )

  def getChainTip: Try[ChainTip] = Try {
    val chainTip =
      ChainTip(
        blockIdsByHeight
          .iterator(None, None, reverse = true)
          .take(100)
          .flatMap { case (_, blockIds) =>
            blockIds.asScala.flatMap(b => blockById.get(b).map(b -> _))
          }
      )
    val sortedKeys = chainTip.toMap.values.map(_.height).toSeq.sorted
    assert(
      sortedKeys.lastOption == getLastHeight,
      s"MvStore's Iterator works unexpectedly, ${sortedKeys.mkString(", ")} but last key is $getLastHeight!"
    )
    chainTip
  }

  private def clearEmptySuperNodes(): Try[Unit] =
    utxosByErgoTreeHex.clearEmptySuperNodes().flatMap { _ =>
      utxosByErgoTreeT8Hex.clearEmptySuperNodes()
    }

  def compact(
    indexing: Boolean
  ): Task[Unit] = ZIO.attempt {
    logger.info(s"Compacting file at $getCompactReport")
    val compactTime = if (indexing) mvStoreConf.maxIndexingCompactTime else mvStoreConf.maxIdleCompactTime
    val result      = if (!indexing) clearEmptySuperNodes() else Success(())
    result.map(_ => store.compactFile(compactTime.toMillis.toInt))
  }

  private def getCompactReport: String = {
    val height                                                      = getLastHeight.getOrElse(0)
    val MultiColSize(superNodeSize, superNodeTotalSize, commonSize) = utxosByErgoTreeHex.size
    val nonEmptyAddressCount                                        = superNodeSize + commonSize
    val progress =
      s"storage height: $height, " +
        s"utxo count: ${ergoTreeHexByUtxo.size}, " +
        s"supernode-utxo-count : $superNodeTotalSize, " +
        s"non-empty-address count: $nonEmptyAddressCount \n"
    val cs  = store.getCacheSize
    val csu = store.getCacheSizeUsed
    val chr = store.getCacheHitRatio
    val cc  = store.getChunkCount
    val cfr = store.getChunksFillRate
    val fr  = store.getFillRate
    val lr  = store.getLeafRatio
    val pc  = store.getPageCount
    val mps = store.getMaxPageSize
    val kpp = store.getKeysPerPage
    val debug =
      s"cache size used: $csu from: $cs at ratio: $chr, chunks: $cc at fill rate: $cfr, fill rate: $fr, " +
        s"leaf ratio: $lr, page count: $pc, max page size: $mps, keys per page: $kpp"
    progress + debug
  }

  def writeReportAndCompact(blocksIndexed: Int): Task[Unit] =
    if (blocksIndexed > 100000) {
      getReportByPath.foreach { case (path, report) =>
        val lines =
          report.map { case (hotKey, SuperNodeCounter(writeOps, readOps, boxesAdded, boxesRemoved)) =>
            val stats  = s"$writeOps $readOps $boxesAdded $boxesRemoved ${boxesAdded - boxesRemoved}"
            val indent = 45
            s"$stats ${List.fill(Math.max(4, indent - stats.length))(" ").mkString("")} $hotKey"
          }
        tool.FileUtils.writeReport(lines, path).recover { case ex =>
          logger.error(s"Failing to write report", ex)
        }
      }
      compact(true).logError
    } else ZIO.succeed(())

  def commit(): Revision = store.commit()

  def rollbackTo(rev: Revision): Unit = store.rollbackTo(rev)

  def removeInputBoxesByErgoTree(inputRecords: InputRecords): Try[_] = Try {
    inputRecords.byErgoTree.iterator
      .map { case (ergoTreeHex, inputIds) =>
        val inputBoxes =
          inputIds.filter { boxId =>
            boxId != Emission.inputBox && boxId != Foundation.inputBox
          }
        ergoTreeHexByUtxo
          .removeAllOrFail(inputBoxes)
          .flatMap { _ =>
            utxosByErgoTreeHex.removeSubsetOrFail(ergoTreeHex, inputBoxes.iterator, inputBoxes.size) { existingBoxIds =>
              inputBoxes.iterator.foreach(existingBoxIds.remove)
              Option(existingBoxIds).collect { case m if !m.isEmpty => m }
            }
          }
          .get
      }
  }

  def removeInputBoxesByErgoTreeT8(inputRecords: InputRecords): Try[_] = Try {
    inputRecords.byErgoTreeT8.iterator
      .map { case (ergoTreeT8, inputIds) =>
        val inputBoxes = inputIds.filter(boxId => boxId != Emission.inputBox && boxId != Foundation.inputBox)
        utxosByErgoTreeT8Hex
          .removeSubsetOrFail(ergoTreeT8, inputBoxes, inputBoxes.size) { existingBoxIds =>
            inputBoxes.foreach(existingBoxIds.remove)
            Option(existingBoxIds).collect { case m if !m.isEmpty => m }
          }
          .get
      }
  }

  def persistErgoTreeT8Utxos(outputRecords: OutputRecords): Try[_] = Try {
    outputRecords.byErgoTreeT8
      .collect { case (ergoTreeT8, boxes) =>
        utxosByErgoTreeT8Hex
          .adjustAndForget(
            ergoTreeT8.hex,
            boxes.iterator.map(_.boxId),
            boxes.size
          )
          .get
      }
  }

  def persistErgoTreeUtxos(outputRecords: OutputRecords): Try[_] = Try {
    outputRecords.byErgoTree
      .map { case (ergoTreeHex, boxes) =>
        ergoTreeHexByUtxo
          .putAllNewOrFail(boxes.iterator.map(b => b.boxId -> ergoTreeHex.hex))
          .flatMap { _ =>
            utxosByErgoTreeHex.adjustAndForget(ergoTreeHex.hex, boxes.iterator.map(_.boxId), boxes.size)
          }
          .get
      }
  }

  def insertNewBlock(
    blockId: BlockId,
    block: Block,
    currentVersion: Revision
  ): Try[Set[BlockId]] =
    blockById
      .putIfAbsentOrFail(blockId, block.persistable(currentVersion))
      .map { _ =>
        blockIdsByHeight
          .adjust(block.height)(
            _.fold(javaSetOf(blockId)) { existingBlockIds =>
              existingBlockIds.add(blockId)
              existingBlockIds
            }
          )
      }
      .map { blockIds =>
        val r = blockIds.asScala.toSet
        if (blockIds.size > 1) logger.info(s"Fork at height ${block.height} with ${r.mkString(", ")}")
        r
      }

  def getBlocksByHeight(atHeight: Height): Map[BlockId, Block] =
    blockIdsByHeight
      .get(atHeight)
      .map(_.asScala.flatMap(blockId => blockById.get(blockId).map(blockId -> _)).toMap)
      .getOrElse(Map.empty)

  def isEmpty: Boolean =
    utxosByErgoTreeHex.isEmpty && ergoTreeHexByUtxo.isEmpty && blockIdsByHeight.isEmpty && blockById.isEmpty

  def getLastHeight: Option[Height] = blockIdsByHeight.lastKey

  def getLastBlocks: Map[BlockId, Block] =
    blockIdsByHeight.lastKey
      .map { lastHeight =>
        getBlocksByHeight(lastHeight)
      }
      .getOrElse(Map.empty)

  def containsBlock(blockId: BlockId, atHeight: Height): Boolean =
    blockById.containsKey(blockId) && blockIdsByHeight.containsKey(atHeight)

  def getBlockById(blockId: BlockId): Option[Block] = blockById.get(blockId)

  def getErgoTreeHexByUtxo(boxId: BoxId): Option[ErgoTreeHex] = ergoTreeHexByUtxo.get(boxId)

  def findMissingHeights: TreeSet[Height] = {
    val lastHeight = getLastHeight
    if (lastHeight.isEmpty || lastHeight.contains(1))
      TreeSet.empty
    else
      TreeSet(1 to lastHeight.get: _*).diff(blockIdsByHeight.keySet.asScala)
  }

  override def getCurrentRevision: Revision = store.getCurrentVersion

}

object MvStorage extends LazyLogging {
  import scala.concurrent.duration.*

  private val VersionsToKeep = 10

  private def zlayer(dbFile: File): ZLayer[MvStoreConf, Throwable, MvStorage] =
    ZLayer.service[MvStoreConf].flatMap { mvStoreConf =>
      ZLayer.scoped(
        ZIO.acquireRelease {
          ZIO.attempt {
            dbFile.getParentFile.mkdirs()
            implicit val store: MVStore =
              new MVStore.Builder()
                .fileName(dbFile.getAbsolutePath)
                .cacheSize(mvStoreConf.get.cacheSize)
                .cacheConcurrency(4)
                .autoCommitDisabled()
                .open()

            store.setVersionsToKeep(VersionsToKeep)
            store.setRetentionTime(3600 * 1000 * 24 * 7)

            logger.info(s"Opening mvstore at version ${store.getCurrentVersion}")
            MvStorage(
              multiset.MultiMvSet[ErgoTreeHex, util.Set, BoxId]("utxosByErgoTreeHex"),
              multiset.MultiMvSet[ErgoTreeT8Hex, util.Set, BoxId]("utxosByErgoTreeT8Hex"),
              MvMap[BoxId, ErgoTreeHex]("ergoTreeHexByUtxo"),
              MvMap[Height, util.Set[BlockId]]("blockIdsByHeight"),
              MvMap[BlockId, Block]("blockById")
            )(store, mvStoreConf.get)
          }
        } { storage =>
          logger.info(s"Closing mvstore at version ${storage.store.getCurrentVersion}")
          ZIO.succeed(storage.store.close())
        }
      )
    }

  def zlayerWithTempDir: ZLayer[MvStoreConf, Throwable, MvStorage] = zlayer(
    tempDir.resolve(s"mv-store-$randomNumberPerRun.db").toFile
  )

  def zlayerWithDefaultDir: ZLayer[MvStoreConf, Throwable, MvStorage] = zlayer(ergoHomeDir.resolve("mv-store.db").toFile)
}
