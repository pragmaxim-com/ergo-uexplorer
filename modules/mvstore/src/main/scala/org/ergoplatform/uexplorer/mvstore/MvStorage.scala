package org.ergoplatform.uexplorer.mvstore

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferInput, ByteBufferOutput, Input, Output}
import com.esotericsoftware.kryo.serializers.MapSerializer
import com.esotericsoftware.kryo.util.Pool
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.shaded.kryo.pool.KryoPool
import org.ergoplatform.uexplorer.*
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
  blockById: MapLike[BlockId, BlockMetadata]
) extends Storage
  with LazyLogging {

  def close(): Try[Unit] = Try(store.close())

  def compact(moveChunks: Boolean): Try[Unit] = Try {
    store.compactFile(60000 * 10)
    if (moveChunks) {
      store.compactMoveChunks()
    }
  }

  def rollbackTo(version: Long): Try[Unit] = Try(store.rollbackTo(version))

  def removeInputBoxesByAddress(address: Address, inputBoxes: Iterable[BoxId]): Unit = {
    inputBoxes.foreach(addressByUtxo.removeAndForget)
    utxosByAddress.putOrRemoveAndForget(address) {
      case None => None
      case Some(existingBoxIds) =>
        inputBoxes.foreach(existingBoxIds.remove)
        Option(existingBoxIds).collect { case m if !m.isEmpty => m }
    }
  }

  def persistBox(boxId: BoxId, address: Address, value: Value): Unit = { // Without Try for perf reasons
    addressByUtxo.putAndForget(boxId, address)
    utxosByAddress.adjustAndForget(address)(_.fold(javaMapOf(boxId, value)) { arr =>
      arr.put(boxId, value)
      arr
    })
  }

  def persistNewBlock(blockId: BlockId, height: Height, block: BlockMetadata): Try[Unit] = Try {
    blockById.putAndForget(blockId, block)
    blockIdsByHeight.adjustAndForget(height)(
      _.fold(javaSetOf(blockId)) { existingBlockIds =>
        existingBlockIds.add(blockId)
        existingBlockIds
      }
    )
  }

  def getReport: String = {
    val height = getLastHeight.getOrElse(0)
    s"Storage height $height, utxo count: ${addressByUtxo.size}, non-empty-address count: ${utxosByAddress.size}"
  }

  def getBlocksByHeight(atHeight: Height): Map[BlockId, BlockMetadata] =
    blockIdsByHeight
      .get(atHeight)
      .map(_.asScala.flatMap(blockId => blockById.get(blockId).map(blockId -> _)).toMap)
      .getOrElse(Map.empty)

  def getUtxosByAddress(address: Address): Option[Map[BoxId, Value]] =
    utxosByAddress.get(address).map(_.asScala.toMap)

  def isEmpty: Boolean =
    utxosByAddress.isEmpty && addressByUtxo.isEmpty && blockIdsByHeight.isEmpty && blockById.isEmpty

  def getLastHeight: Option[Height] = blockIdsByHeight.lastKey

  def getLastBlocks: Map[BlockId, BlockMetadata] =
    blockIdsByHeight.lastKey
      .map { lastHeight =>
        getBlocksByHeight(lastHeight)
      }
      .getOrElse(Map.empty)

  def containsBlock(blockId: BlockId, atHeight: Height): Boolean =
    blockById.get(blockId).exists(_.height == atHeight)

  def getBlockById(blockId: BlockId): Option[BlockMetadata] = blockById.get(blockId)

  def getAddressByUtxo(boxId: BoxId): Option[Address] = addressByUtxo.get(boxId)

  def findMissingHeights: TreeSet[Height] = {
    val lastHeight = getLastHeight
    if (lastHeight.isEmpty || lastHeight.contains(1))
      TreeSet.empty
    else
      TreeSet((1 to lastHeight.get): _*).diff(blockIdsByHeight.keySet.asScala)
  }

  override def getCurrentVersion: Long = store.getCurrentVersion
}

object MvStorage extends LazyLogging {
  val VersionsToKeep  = 10
  val CompactFileRate = 10000
  val MoveChunksRate  = 100000

  def apply(
    rootDir: File = Paths.get(System.getProperty("java.io.tmpdir"), Random.nextString(10)).toFile
  ): Try[MvStorage] = Try {
    rootDir.mkdirs()
    val store =
      new MVStore.Builder()
        .fileName(rootDir.toPath.resolve("mvstore").toFile.getAbsolutePath)
        .autoCommitDisabled()
        .open()

    store.setVersionsToKeep(VersionsToKeep)
    store.setRetentionTime(3600 * 1000 * 24 * 7)
    MvStorage(
      store,
      new MVMap4S[Address, java.util.Map[BoxId, Value]]("utxosByAddress", store),
      new MVMap4S[BoxId, Address]("addressByUtxo", store),
      new MVMap4S[Height, java.util.Set[BlockId]]("blockIdsByHeight", store),
      new MVMap4S[BlockId, BlockMetadata]("blockById", store)
    )
  }

  def withDefaultDir(): Try[MvStorage] =
    MvStorage(Paths.get(System.getProperty("user.home"), ".ergo-uexplorer", "utxo").toFile)
}
