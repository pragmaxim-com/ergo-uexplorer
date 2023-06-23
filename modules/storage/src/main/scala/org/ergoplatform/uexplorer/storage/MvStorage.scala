package org.ergoplatform.uexplorer.storage

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferInput, ByteBufferOutput, Input, Output}
import com.esotericsoftware.kryo.serializers.MapSerializer
import com.esotericsoftware.kryo.util.Pool
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.shaded.kryo.pool.KryoPool
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.Const.Protocol.{Emission, FeeContract, Foundation}
import org.ergoplatform.uexplorer.mvstore.*
import MvStorage.*
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.mvstore.MultiMapLike.MultiMapSize
import org.ergoplatform.uexplorer.storage.Implicits.*
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ApiTransaction}
import org.h2.mvstore.{MVMap, MVStore}
import org.ergoplatform.uexplorer.db.OutputRecord

import java.io.{BufferedInputStream, File}
import java.nio.ByteBuffer
import java.nio.file.{CopyOption, Files, Paths}
import java.util
import java.util.Map.Entry
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap}
import scala.collection.concurrent
import java.util.stream.Collectors
import java.util.zip.GZIPInputStream
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.{TreeMap, TreeSet}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.io.Source
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Random, Success, Try}

case class MvStorage(
  store: MVStore,
  utxosByErgoTreeHex: MultiMvMap[ErgoTreeHex, java.util.Map, BoxId, Value],
  utxosByErgoTreeT8Hex: MultiMvMap[ErgoTreeT8Hex, java.util.Map, BoxId, CreationHeight],
  ergoTreeHexByUtxo: MapLike[BoxId, ErgoTreeHex],
  blockIdsByHeight: MapLike[Height, java.util.Set[BlockId]],
  blockById: MapLike[BlockId, BlockInfo]
) extends Storage
  with LazyLogging {

  def clear(): Try[Unit] =
    utxosByErgoTreeHex.clear()

  def close(): Try[Unit] = Try(store.close())

  def rollbackTo(version: Revision): Try[Unit] = Try(store.rollbackTo(version))

  def removeInputBoxesByErgoTree(inputRecords: InputRecords): Try[_] = Try {
    inputRecords.byErgoTree.iterator
      .map { case (ergoTreeHex, inputIds) =>
        val inputBoxes =
          inputIds.filter { case (boxId, _) =>
            boxId != Emission.inputBox && boxId != Foundation.inputBox
          }
        ergoTreeHexByUtxo
          .removeAllOrFail(inputBoxes.keys)
          .flatMap { _ =>
            utxosByErgoTreeHex.removeAllOrFail(ergoTreeHex, inputBoxes.iterator.map(_._1), inputBoxes.size) {
              existingBoxIds =>
                inputBoxes.iterator.map(_._1).foreach(existingBoxIds.remove)
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
          .removeAllOrFail(ergoTreeT8, inputBoxes, inputBoxes.size) { existingBoxIds =>
            inputBoxes.foreach(existingBoxIds.remove)
            Option(existingBoxIds).collect { case m if !m.isEmpty => m }
          }
          .get
      }
  }

  def persistErgoTreeTemplateUtxos(outputRecords: ArraySeq[OutputRecord]): Try[_] = Try {
    outputRecords
      .filter(_.ergoTreeT8Hex.isDefined)
      .groupBy(_.ergoTreeT8Hex)
      .collect { case (Some(ergoTreeT8Hex), boxes) =>
        utxosByErgoTreeT8Hex
          .adjustAndForget(
            ergoTreeT8Hex,
            boxes.iterator.map(b => b.boxId -> b.creationHeight),
            boxes.size
          )
          .get
      }
  }

  def persistErgoTreeUtxos(outputRecords: ArraySeq[OutputRecord]): Try[_] = Try {
    outputRecords
      .groupBy(_.ergoTreeHex)
      .map { case (ergoTreeHex, boxes) =>
        ergoTreeHexByUtxo
          .putAllNewOrFail(boxes.iterator.map(b => b.boxId -> b.ergoTreeHex))
          .flatMap { _ =>
            utxosByErgoTreeHex.adjustAndForget(ergoTreeHex, boxes.iterator.map(b => b.boxId -> b.value), boxes.size)
          }
          .get
      }
  }

  def commitNewBlock(
    blockId: BlockId,
    blockInfo: BlockInfo,
    mvStoreConf: MvStoreConf,
    currentVersion: Revision
  ): Try[Set[BlockId]] =
    blockById
      .putIfAbsentOrFail(blockId, blockInfo.persistable(currentVersion))
      .map { _ =>
        blockIdsByHeight
          .adjust(blockInfo.height)(
            _.fold(javaSetOf(blockId)) { existingBlockIds =>
              existingBlockIds.add(blockId)
              existingBlockIds
            }
          )
      }
      .map { blockIds =>
        store.commit()
        val r = blockIds.asScala.toSet
        if (blockIds.size > 1) logger.info(s"Fork at height ${blockInfo.height} with ${r.mkString(", ")}")
        if (blockInfo.height % mvStoreConf.heightCompactRate == 0)
          compact(true, mvStoreConf.maxIndexingCompactTime, mvStoreConf.maxIdleCompactTime)
        else Success(())
        r
      }

  def writeReport: Try[_] = utxosByErgoTreeHex.writeReport

  def compact(
    indexing: Boolean,
    maxIndexingCompactTime: MaxCompactTime,
    maxIdleCompactTime: MaxCompactTime
  ): Try[Unit] = Try {
    logger.info(s"Compacting file at $getCompactReport")
    val compactTime = if (indexing) maxIndexingCompactTime else maxIdleCompactTime
    val result      = if (!indexing) clear() else Success(())
    result.map(_ => store.compactFile(compactTime.toMillis.toInt))
  }

  def getCompactReport: String = {
    val height                                                      = getLastHeight.getOrElse(0)
    val MultiMapSize(superNodeSize, superNodeTotalSize, commonSize) = utxosByErgoTreeHex.size
    val nonEmptyAddressCount                                        = superNodeSize + commonSize
    val progress =
      s"storage height: $height, " +
        s"utxo count: ${ergoTreeHexByUtxo.size}, " +
        s"supernode-utxo-count : $superNodeTotalSize, " +
        s"non-empty-address count: $nonEmptyAddressCount, "

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

  def getBlocksByHeight(atHeight: Height): Map[BlockId, BlockInfo] =
    blockIdsByHeight
      .get(atHeight)
      .map(_.asScala.flatMap(blockId => blockById.get(blockId).map(blockId -> _)).toMap)
      .getOrElse(Map.empty)

  def getUtxosByErgoTreeHex(ergoTreeHex: ErgoTreeHex): Option[java.util.Map[BoxId, Value]] =
    utxosByErgoTreeHex.getAll(ergoTreeHex)

  def getUtxoValueByErgoTreeHex(ergoTreeHex: ErgoTreeHex, utxo: BoxId): Option[Value] =
    utxosByErgoTreeHex.get(ergoTreeHex, utxo)

  def getUtxoValuesByErgoTreeHex(ergoTreeHex: ErgoTreeHex, utxos: IterableOnce[BoxId]): Option[java.util.Map[BoxId, Value]] =
    utxosByErgoTreeHex.getPartially(ergoTreeHex, utxos)

  def getUtxoValuesByErgoTreeT8Hex(
    ergoTreeHex: ErgoTreeT8Hex,
    utxos: IterableOnce[BoxId]
  ): Option[util.Map[BoxId, CreationHeight]] =
    utxosByErgoTreeT8Hex.getPartially(ergoTreeHex, utxos)

  def isEmpty: Boolean =
    utxosByErgoTreeHex.isEmpty && ergoTreeHexByUtxo.isEmpty && blockIdsByHeight.isEmpty && blockById.isEmpty

  def getLastHeight: Option[Height] = blockIdsByHeight.lastKey

  def getLastBlocks: Map[BlockId, BlockInfo] =
    blockIdsByHeight.lastKey
      .map { lastHeight =>
        getBlocksByHeight(lastHeight)
      }
      .getOrElse(Map.empty)

  def containsBlock(blockId: BlockId, atHeight: Height): Boolean =
    blockById.containsKey(blockId) && blockIdsByHeight.containsKey(atHeight)

  def getBlockById(blockId: BlockId): Option[BlockInfo] = blockById.get(blockId)

  def getErgoTreeHexByUtxo(boxId: BoxId): Option[ErgoTreeHex] = ergoTreeHexByUtxo.get(boxId)

  def findMissingHeights: TreeSet[Height] = {
    val lastHeight = getLastHeight
    if (lastHeight.isEmpty || lastHeight.contains(1))
      TreeSet.empty
    else
      TreeSet((1 to lastHeight.get): _*).diff(blockIdsByHeight.keySet.asScala)
  }

  override def getCurrentRevision: Revision = store.getCurrentVersion
}

object MvStorage extends LazyLogging {
  import scala.concurrent.duration.*
  import SuperNodeCollector.randomNumberPerRun

  type CacheSize         = Int
  type HeightCompactRate = Int
  type MaxCompactTime    = FiniteDuration

  private val userHomeDir                = System.getProperty("user.home")
  private val tempDir                    = System.getProperty("java.io.tmpdir")
  private val VersionsToKeep             = 10
  private val dbFileName                 = "mv-store.db"
  private val dbFile                     = Paths.get(userHomeDir, ".ergo-uexplorer", dbFileName).toFile
  private val tempDbFile                 = Paths.get(tempDir, s"mv-store-$randomNumberPerRun.db").toFile
  private val tempOutputHotErgoTreesFile = Paths.get(tempDir, s"hot-ergo-trees-$randomNumberPerRun.csv").toFile
  private val prodOutputHotErgoTreesFile = Paths.get(userHomeDir, ".ergo-uexplorer", "hot-ergo-trees.csv").toFile
  private val tempOutputHotTemplatesFile = Paths.get(tempDir, s"hot-templates-$randomNumberPerRun.csv").toFile
  private val prodOutputHotTemplatesFile = Paths.get(userHomeDir, ".ergo-uexplorer", "hot-templates.csv").toFile

  def apply(
    cacheSize: CacheSize,
    dbFile: File              = tempDbFile,
    outHotErgoTreesFile: File = tempOutputHotErgoTreesFile,
    outHotTemplatesFile: File = tempOutputHotTemplatesFile
  ): Try[MvStorage] = Try {
    dbFile.getParentFile.mkdirs()
    outHotErgoTreesFile.getParentFile.mkdirs()
    if (outHotErgoTreesFile.exists()) {
      val backupOutputPath = outHotErgoTreesFile.toPath.resolve(".backup")
      logger.warn(s"Moving file ${outHotErgoTreesFile.getAbsolutePath} to ${backupOutputPath.toFile.getAbsolutePath}")
      Files.move(outHotErgoTreesFile.toPath, backupOutputPath)
    }
    val store =
      new MVStore.Builder()
        .fileName(dbFile.getAbsolutePath)
        .cacheSize(cacheSize)
        .cacheConcurrency(2)
        .autoCommitDisabled()
        .open()

    logger.info(s"Opening mvstore at version ${store.getCurrentVersion}")

    store.setVersionsToKeep(VersionsToKeep)
    store.setRetentionTime(3600 * 1000 * 24 * 7)
    MvStorage(
      store,
      new MultiMvMap[ErgoTreeHex, util.Map, BoxId, Value](
        new MvMap[ErgoTreeHex, util.Map[BoxId, Value]]("utxosByErgoTreeHex", store),
        SuperNodeMvMap[ErgoTreeHex, util.Map, BoxId, Value](store, "hot-ergo-trees.csv.gz", outHotErgoTreesFile)
      ),
      new MultiMvMap[ErgoTreeT8Hex, util.Map, BoxId, CreationHeight](
        new MvMap[ErgoTreeT8Hex, util.Map[BoxId, CreationHeight]]("utxosByErgoTreeTemplateHex", store),
        SuperNodeMvMap[ErgoTreeT8Hex, util.Map, BoxId, CreationHeight](store, "hot-templates.csv.gz", outHotTemplatesFile)
      ),
      new MvMap[BoxId, ErgoTreeHex]("ergoTreeHexByUtxo", store),
      new MvMap[Height, util.Set[BlockId]]("blockIdsByHeight", store),
      new MvMap[BlockId, BlockInfo]("blockById", store)
    )
  }

  def withDefaultDir(cacheSize: CacheSize): Try[MvStorage] =
    MvStorage(
      cacheSize,
      dbFile,
      outHotErgoTreesFile = prodOutputHotErgoTreesFile,
      outHotTemplatesFile = prodOutputHotTemplatesFile
    )
}
