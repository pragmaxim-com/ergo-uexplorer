package org.ergoplatform.uexplorer.storage

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferInput, ByteBufferOutput, Input, Output}
import com.esotericsoftware.kryo.serializers.MapSerializer
import com.esotericsoftware.kryo.util.Pool
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
import scala.collection.immutable.{ArraySeq, TreeMap, TreeSet}
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
  ergoTreeT8HexByUtxo: MapLike[BoxId, ErgoTreeT8Hex],
  blockIdsByHeight: MapLike[Height, java.util.Set[BlockId]],
  blockById: MapLike[BlockId, Block]
)(implicit val store: MVStore, mvStoreConf: MvStoreConf)
  extends WritableStorage {

  private def getReportByPath: Map[Path, Vector[(String, SuperNodeCounter)]] =
    Map(
      utxosByErgoTreeHex.getReport,
      utxosByErgoTreeT8Hex.getReport
    )

  def getChainTip: Task[ChainTip] = {
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
    if (sortedKeys.lastOption != getLastHeight)
      ZIO.fail(
        new IllegalStateException(
          s"MvStore's Iterator works unexpectedly, ${sortedKeys.mkString(", ")} but last key is $getLastHeight!"
        )
      )
    else
      ZIO.succeed(chainTip) <* ZIO.log(s"Chain tip from ${sortedKeys.headOption} to ${sortedKeys.lastOption}")
  }

  private def clearEmptySuperNodes: Task[Unit] =
    utxosByErgoTreeHex.clearEmptySuperNodes *> utxosByErgoTreeT8Hex.clearEmptySuperNodes

  def compact(
    indexing: Boolean
  ): Task[Unit] =
    for {
      _ <- ZIO.log(s"Compacting file at $getCompactReport")
      _ <- ZIO.unless(indexing)(clearEmptySuperNodes)
      compactTime = if (indexing) mvStoreConf.maxIndexingCompactTime else mvStoreConf.maxIdleCompactTime
      _ <- ZIO.attempt(store.compactFile(compactTime.toMillis.toInt))
    } yield ()

  private def getCompactReport: String = {
    val height                                                      = getLastHeight.getOrElse(0)
    val MultiColSize(superNodeSize, superNodeTotalSize, commonSize) = utxosByErgoTreeHex.size
    val nonEmptyAddressCount                                        = superNodeSize + commonSize
    val progress =
      s"storage height: $height, " +
        s"utxo count: ${ergoTreeHexByUtxo.size}, " +
        s"utxo with template count: ${ergoTreeT8HexByUtxo.size}, " +
        s"supernode-utxo-count : $superNodeTotalSize, " +
        s"non-empty-address count: $nonEmptyAddressCount "
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
      ZIO.collectAllDiscard(
        getReportByPath.map { case (path, report) =>
          val lines = report.map { case (hotKey, SuperNodeCounter(writeOps, readOps, boxesAdded, boxesRemoved)) =>
            val stats  = s"$writeOps $readOps $boxesAdded $boxesRemoved ${boxesAdded - boxesRemoved}"
            val indent = 45
            s"$stats ${List.fill(Math.max(4, indent - stats.length))(" ").mkString("")} $hotKey"
          }
          tool.FileUtils.writeReport(lines, path)
        }
      ) *> compact(true)
    } else ZIO.succeed(())

  def commit(): Revision = store.commit()

  def rollbackTo(rev: Revision): Unit = store.rollbackTo(rev)

  def removeInputBoxesByErgoTree(transactions: ArraySeq[ApiTransaction]): Task[_] = ZIO.attempt {
    val inputIds = transactions.flatMap(_.inputs.map(_.boxId))
    inputIds
      .flatMap { inputId =>
        ergoTreeHexByUtxo.get(inputId).map(_ -> inputId)
      }
      .groupBy(_._1)
      .foreach { case (et, inputBoxes) =>
        utxosByErgoTreeHex.removeSubsetOrFail(et, inputBoxes.iterator.map(_._2), inputBoxes.size) { existingBoxIds =>
          inputBoxes.iterator.foreach(t => existingBoxIds.remove(t._2))
          Option(existingBoxIds).collect { case m if !m.isEmpty => m }
        }
      }

    ergoTreeHexByUtxo
      .removeAllOrFail(inputIds)

  }

  def removeInputBoxesByErgoTreeT8(transactions: ArraySeq[ApiTransaction]): Task[_] = ZIO.attempt {
    val inputIds = transactions.flatMap(_.inputs.map(_.boxId))
    inputIds
      .flatMap { inputId =>
        ergoTreeT8HexByUtxo.get(inputId).map(_ -> inputId)
      }
      .groupBy(_._1)
      .foreach { case (etT8, inputBoxes) =>
        utxosByErgoTreeT8Hex.removeSubsetOrFail(etT8, inputBoxes.iterator.map(_._2), inputBoxes.size) { existingBoxIds =>
          inputBoxes.iterator.foreach(t => existingBoxIds.remove(t._2))
          Option(existingBoxIds).collect { case m if !m.isEmpty => m }
        }
      }

    ergoTreeT8HexByUtxo
      .removeAllOrFail(inputIds)
  }

  // TODO parallel writes to 2 different NvMaps
  def persistErgoTreeByUtxo(outputRecords: OutputRecords): Task[_] = ZIO.attempt {
    outputRecords.byErgoTree
      .foreach { case (ergoTreeHex, boxes) =>
        ergoTreeHexByUtxo
          .putAllNewOrFail(boxes.iterator.map(b => b.boxId -> ergoTreeHex.hex))
          .flatMap { _ =>
            utxosByErgoTreeHex.adjustAndForget(ergoTreeHex.hex, boxes.iterator.map(_.boxId), boxes.size)
          }
          .get
      }
  }
  def persistErgoTreeT8ByUtxo(outputRecords: OutputRecords): Task[_] = ZIO.attempt {
    outputRecords.byErgoTreeT8
      .foreach { case (ergoTreeT8, boxes) =>
        ergoTreeT8HexByUtxo
          .putAllNewOrFail(boxes.iterator.map(b => b.boxId -> ergoTreeT8.hex))
          .flatMap { _ =>
            utxosByErgoTreeT8Hex.adjustAndForget(ergoTreeT8.hex, boxes.iterator.map(_.boxId), boxes.size)
          }
          .get
      }
  }

  def insertNewBlock(
    blockId: BlockId,
    block: Block,
    currentVersion: Revision
  ): Task[Set[BlockId]] =
    blockById
      .putIfAbsentOrFail(blockId, block.persistable(currentVersion))
      .as {
        blockIdsByHeight
          .adjust(block.height)(
            _.fold(javaSetOf(blockId)) { existingBlockIds =>
              existingBlockIds.add(blockId)
              existingBlockIds
            }
          )
      }
      .map(_.asScala.toSet)
      .tap {
        case blockIds if blockIds.size > 1 => ZIO.log(s"Fork at height ${block.height} with ${blockIds.mkString(", ")}")
        case _                             => ZIO.unit
      }

  def getBlocksByHeight(atHeight: Height): Map[BlockId, Block] =
    blockIdsByHeight
      .get(atHeight)
      .map(_.asScala.flatMap(blockId => blockById.get(blockId).map(blockId -> _)).toMap)
      .getOrElse(Map.empty)

  def isEmpty: Boolean =
    utxosByErgoTreeHex.isEmpty &&
      ergoTreeHexByUtxo.isEmpty &&
      ergoTreeT8HexByUtxo.isEmpty &&
      blockIdsByHeight.isEmpty &&
      blockById.isEmpty

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

  def getErgoTreeT8HexByUtxo(boxId: BoxId): Option[ErgoTreeT8Hex] = ergoTreeT8HexByUtxo.get(boxId)

  def findMissingHeights: TreeSet[Height] = {
    val lastHeight = getLastHeight
    if (lastHeight.isEmpty || lastHeight.contains(1))
      TreeSet.empty
    else
      TreeSet(1 to lastHeight.get: _*).diff(blockIdsByHeight.keySet.asScala)
  }

  override def getCurrentRevision: Revision = store.getCurrentVersion

}

object MvStorage {
  import scala.concurrent.duration.*

  private val VersionsToKeep = 10

  private def zlayer(dbFile: File): ZLayer[MvStoreConf, Throwable, MvStorage] =
    ZLayer.service[MvStoreConf].flatMap { mvStoreConf =>
      ZLayer.scoped(
        ZIO.acquireRelease {
          ZIO
            .attempt {
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
              store
            }
            .tap(store => ZIO.log(s"Opened mvstore at version ${store.getCurrentVersion}"))
            .map { implicit store =>
              MvStorage(
                multiset.MultiMvSet[ErgoTreeHex, util.Set, BoxId]("utxosByErgoTreeHex"),
                multiset.MultiMvSet[ErgoTreeT8Hex, util.Set, BoxId]("utxosByErgoTreeT8Hex"),
                MvMap[BoxId, ErgoTreeHex]("ergoTreeHexByUtxo"),
                MvMap[BoxId, ErgoTreeT8Hex]("ergoTreeT8HexByUtxo"),
                MvMap[Height, util.Set[BlockId]]("blockIdsByHeight"),
                MvMap[BlockId, Block]("blockById")
              )(store, mvStoreConf.get)
            }
        } { storage =>
          ZIO.log(s"Closing mvstore at version ${storage.store.getCurrentVersion}") *> ZIO.succeed(storage.store.close())
        }
      )
    }

  def zlayerWithTempDir: ZLayer[MvStoreConf, Throwable, MvStorage] = zlayer(
    tempDir.resolve(s"mv-store-$randomNumberPerRun.db").toFile
  )

  def zlayerWithDefaultDir: ZLayer[MvStoreConf, Throwable, MvStorage] = zlayer(ergoHomeDir.resolve("mv-store.db").toFile)
}
