package org.ergoplatform.uexplorer.mvstore

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferInput, ByteBufferOutput, Input, Output}
import com.esotericsoftware.kryo.serializers.MapSerializer
import com.esotericsoftware.kryo.util.Pool
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.shaded.kryo.pool.KryoPool
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.Const.Genesis.{Emission, Foundation}
import org.ergoplatform.uexplorer.db.{BlockInfo, LightBlock}
import org.ergoplatform.uexplorer.mvstore.*
import org.ergoplatform.uexplorer.mvstore.MvStorage.*
import org.ergoplatform.uexplorer.mvstore.kryo.KryoSerialization.Implicits.*
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ApiTransaction}
import org.ergoplatform.uexplorer.mvstore.MvStorage
import org.h2.mvstore.{MVMap, MVStore}

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util
import java.util.concurrent.ConcurrentSkipListMap
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.{TreeMap, TreeSet}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Random, Success, Try}

case class MvStorage(
  store: MVStore,
  utxosByAddress: MapLike[Address, java.util.Map[BoxId, Value]],
  addressByUtxo: MapLike[BoxId, Address],
  blockIdsByHeight: MapLike[Height, java.util.Set[BlockId]],
  blockById: MapLike[BlockId, BlockInfo]
) extends Storage
  with LazyLogging {

  def close(): Try[Unit] = Try(store.close())

  def rollbackTo(version: Revision): Try[Unit] = Try(store.rollbackTo(version))

  private def removeInputBoxesByAddress(address: Address, inputBoxes: Iterable[BoxId]): Try[Unit] =
    // removeAll is a heavy hitter : 30% of runtime
    addressByUtxo.removeAllOrFail(inputBoxes).flatMap { _ =>
      utxosByAddress
        .removeOrUpdateOrFail(address) { existingBoxIds =>
          inputBoxes.foreach(existingBoxIds.remove)
          Option(existingBoxIds).collect { case m if !m.isEmpty => m }
        }
    }

  private def persistBox(boxId: BoxId, address: Address, value: Value): Unit = { // Without Try for perf reasons
    addressByUtxo.putIfAbsentOrFail(boxId, address).get
    // heavy method, 50% of runtime
    utxosByAddress.adjustAndForget(address)(_.fold(javaMapOf(boxId, value)) { arr =>
      arr.put(boxId, value)
      arr
    })
  }

  def persistNewBlock(lightBlock: LightBlock): Try[LightBlock] = Try {
    lightBlock.boxesByTx
      .foreach { case (_, (inputBoxes, outputBoxes)) =>
        outputBoxes
          .foreach { case (boxId, address, value) =>
            persistBox(boxId, address, value)
          }
        inputBoxes
          .groupBy(_._2)
          .view
          .mapValues(_.collect { case (boxId, _, _) if boxId != Emission.inputBox && boxId != Foundation.box => boxId })
          .foreach { case (address, inputIds) =>
            removeInputBoxesByAddress(address, inputIds).get
          }
      }
    blockById.putIfAbsentAndForget(lightBlock.headerId, lightBlock.info)
    blockIdsByHeight.adjustAndForget(lightBlock.info.height)(
      _.fold(javaSetOf(lightBlock.headerId)) { existingBlockIds =>
        existingBlockIds.add(lightBlock.headerId)
        existingBlockIds
      }
    )
    store.commit()
    lightBlock
  }

  def getReport: String = {
    val height = getLastHeight.getOrElse(0)
    val progress =
      s"storage height $height, utxo count: ${addressByUtxo.size}, non-empty-address count: ${utxosByAddress.size}\n"

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
      s"cache size used $csu from $cs at ratio $chr, chunks $cc at fill rate $cfr, fill rate $fr\n" +
        s"leaf ratio $lr, page count $pc, max page size $mps, keys per page $kpp"
    progress + debug
  }

  def getBlocksByHeight(atHeight: Height): Map[BlockId, BlockInfo] =
    blockIdsByHeight
      .get(atHeight)
      .map(_.asScala.flatMap(blockId => blockById.get(blockId).map(blockId -> _)).toMap)
      .getOrElse(Map.empty)

  def getUtxosByAddress(address: Address): Option[java.util.Map[BoxId, Value]] =
    utxosByAddress.get(address)

  def getUtxoValueByAddress(address: Address, utxo: BoxId): Option[Value] =
    utxosByAddress.get(address).flatMap(m => Option(m.get(utxo)))

  def isEmpty: Boolean =
    utxosByAddress.isEmpty && addressByUtxo.isEmpty && blockIdsByHeight.isEmpty && blockById.isEmpty

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

  def getAddressByUtxo(boxId: BoxId): Option[Address] = addressByUtxo.get(boxId)

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
  import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

  type CompactTime = Int
  type CacheSize   = Int
  val MaxCompactTime         = 10.seconds
  private val VersionsToKeep = 10
  val CompactFileRate        = 10000

  def apply(
    cacheSize: CacheSize,
    rootDir: File = Paths.get(System.getProperty("java.io.tmpdir"), Random.nextString(10)).toFile
  ): Try[MvStorage] = Try {
    rootDir.mkdirs()
    val store =
      new MVStore.Builder()
        .fileName(rootDir.toPath.resolve("mvstore").toFile.getAbsolutePath)
        .cacheSize(cacheSize)
        .cacheConcurrency(2)
        .autoCommitDisabled()
        .open()

    store.setVersionsToKeep(VersionsToKeep)
    store.setRetentionTime(3600 * 1000 * 24 * 7)
    MvStorage(
      store,
      new MVMap4S[Address, java.util.Map[BoxId, Value]]("utxosByAddress", store),
      new MVMap4S[BoxId, Address]("addressByUtxo", store),
      new MVMap4S[Height, java.util.Set[BlockId]]("blockIdsByHeight", store),
      new MVMap4S[BlockId, BlockInfo]("blockById", store)
    )
  }

  def withDefaultDir(cacheSize: CacheSize): Try[MvStorage] =
    MvStorage(cacheSize, Paths.get(System.getProperty("user.home"), ".ergo-uexplorer", "utxo").toFile)
}
