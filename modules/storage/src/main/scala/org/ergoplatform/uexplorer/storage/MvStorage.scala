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
import org.ergoplatform.uexplorer.chain.ChainTip
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.mvstore.MultiMapLike.MultiMapSize
import org.ergoplatform.uexplorer.storage.Implicits.*
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ApiTransaction}
import org.h2.mvstore.{MVMap, MVStore}
import org.ergoplatform.uexplorer.db.OutputRecord
import org.ergoplatform.uexplorer.mvstore.SuperNodeCollector.Counter

import java.io.{BufferedInputStream, File}
import java.nio.ByteBuffer
import java.nio.file.{CopyOption, Files, Path, Paths}
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
  utxosByErgoTreeHex: MultiMvMap[ErgoTreeHex, java.util.Map, BoxId, Value],
  utxosByErgoTreeT8Hex: MultiMvMap[ErgoTreeT8Hex, java.util.Map, BoxId, CreationHeight],
  ergoTreeHexByUtxo: MapLike[BoxId, ErgoTreeHex],
  blockIdsByHeight: MapLike[Height, java.util.Set[BlockId]],
  blockById: MapLike[BlockId, BlockInfo]
)(implicit store: MVStore)
  extends Storage
  with LazyLogging {

  def getReportByPath: Map[Path, Vector[(String, Counter)]] =
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
        r
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
      TreeSet(1 to lastHeight.get: _*).diff(blockIdsByHeight.keySet.asScala)
  }

  override def getCurrentRevision: Revision = store.getCurrentVersion
}

object MvStorage extends LazyLogging {
  import scala.concurrent.duration.*

  private val VersionsToKeep = 10

  def apply(
    cacheSize: CacheSize,
    dbFile: File = tempDir.resolve(s"mv-store-$randomNumberPerRun.db").toFile
  ): Try[MvStorage] = Try {
    dbFile.getParentFile.mkdirs()
    implicit val store: MVStore =
      new MVStore.Builder()
        .fileName(dbFile.getAbsolutePath)
        .cacheSize(cacheSize)
        .cacheConcurrency(4)
        .autoCommitDisabled()
        .open()

    store.setVersionsToKeep(VersionsToKeep)
    store.setRetentionTime(3600 * 1000 * 24 * 7)

    logger.info(s"Opening mvstore at version ${store.getCurrentVersion}")

    MvStorage(
      MultiMvMap[ErgoTreeHex, util.Map, BoxId, Value]("utxosByErgoTreeHex"),
      MultiMvMap[ErgoTreeT8Hex, util.Map, BoxId, CreationHeight]("utxosByErgoTreeT8Hex"),
      MvMap[BoxId, ErgoTreeHex]("ergoTreeHexByUtxo"),
      MvMap[Height, util.Set[BlockId]]("blockIdsByHeight"),
      MvMap[BlockId, BlockInfo]("blockById")
    )
  }

  def withDefaultDir(cacheSize: CacheSize): Try[MvStorage] =
    MvStorage(
      cacheSize,
      ergoHomeDir.resolve("mv-store.db").toFile
    )
}
