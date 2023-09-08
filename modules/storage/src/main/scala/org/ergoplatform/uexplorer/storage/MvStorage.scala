package org.ergoplatform.uexplorer.storage

import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.chain.ChainTip
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.mvstore.*
import org.ergoplatform.uexplorer.mvstore.SuperNodeCounter.{HotKey, NewHotKey}
import org.ergoplatform.uexplorer.mvstore.multimap.MultiMvMap
import org.ergoplatform.uexplorer.mvstore.multiset.MultiMvSet
import org.ergoplatform.uexplorer.storage.Implicits.*
import org.h2.mvstore.MVStore
import zio.*

import java.io.File
import java.nio.file.Path
import java.util
import scala.collection.immutable.TreeSet
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

case class MvStorage(
  utxosByErgoTreeHex: MultiMvMap[ErgoTreeHex, util.Map, BoxId, Value],
  utxosByErgoTreeT8Hex: MultiMvSet[ErgoTreeT8Hex, util.Set, BoxId],
  ergoTreeHexByUtxo: MapLike[BoxId, ErgoTreeHex],
  ergoTreeT8HexByUtxo: MapLike[BoxId, ErgoTreeT8Hex],
  blockIdsByHeight: MapLike[Height, util.Set[BlockId]],
  blockById: MapLike[BlockId, Block],
  utxosByTokenId: MultiMvSet[TokenId, util.Set, BoxId],
  tokensByUtxo: MultiMvMap[BoxId, util.Map, TokenId, Amount]
)(implicit val store: MVStore, mvStoreConf: MvStoreConf)
  extends WritableStorage {

  private def getReportByPath: Map[Path, Vector[HotKey]] =
    Map(
      utxosByErgoTreeHex.getReport,
      utxosByErgoTreeT8Hex.getReport,
      utxosByTokenId.getReport,
      tokensByUtxo.getReport
    )

  def getChainTip: Task[ChainTip] = {
    val lastHeight = getLastHeight
    val chainTip =
      ChainTip(
        blockIdsByHeight
          .iterator(lastHeight.map(lk => Math.max(1, lk - 100)), lastHeight, reverse = false)
          .toSeq
          .sortBy(_._1)(Ordering[Int].reverse)
          .flatMap { case (_, blockIds) =>
            blockIds.asScala.flatMap(b => blockById.get(b).map(b -> _))
          }
      )
    val sortedKeys = chainTip.toMap.values.map(_.height).toSeq.sorted
    if (sortedKeys.lastOption != getLastHeight)
      ZIO.fail(
        new IllegalStateException(
          s"MvStore's Iterator works unexpectedly, ${sortedKeys.mkString(", ")} but last key is $lastHeight!"
        )
      )
    else
      ZIO.succeed(chainTip) <* ZIO.when(sortedKeys.nonEmpty)(ZIO.log(s"Chain tip from ${sortedKeys.headOption} to ${sortedKeys.lastOption}"))
  }

  private def clearEmptySuperNodes: Task[Unit] =
    utxosByErgoTreeHex.clearEmptySuperNodes *> utxosByErgoTreeT8Hex.clearEmptySuperNodes *> utxosByTokenId.clearEmptySuperNodes *> tokensByUtxo.clearEmptySuperNodes

  def compact(
    indexing: Boolean
  ): Task[Unit] =
    for {
      _ <- ZIO.log(s"Compacting file at $getCompactReport")
      compactTime = if (indexing) mvStoreConf.maxIndexingCompactTime else mvStoreConf.maxIdleCompactTime
      _ <- ZIO.attempt(store.compactFile(compactTime.toMillis.toInt))
    } yield ()

  private def getCompactReport: String = {
    val height                   = getLastHeight.getOrElse(0)
    val utxosByErgoTreeHexSize   = utxosByErgoTreeHex.size
    val utxosByErgoTreeT8HexSize = utxosByErgoTreeT8Hex.size
    val utxosByTokenIdSize       = utxosByTokenId.size
    val progress =
      s"storage height: $height, " +
        s"utxo count (supernode/common/total): ${utxosByErgoTreeHexSize.superNodeSum}/${utxosByErgoTreeHexSize.commonSize}/${ergoTreeHexByUtxo.size}, " +
        s"t8-utxo count (supernode/common/total): ${utxosByErgoTreeT8HexSize.superNodeSum}/${utxosByErgoTreeT8HexSize.commonSize}/${ergoTreeT8HexByUtxo.size}, " +
        s"token-utxo count (supernode/common/total): ${utxosByTokenIdSize.superNodeSum}/${utxosByTokenIdSize.commonSize}/${tokensByUtxo.size}, " +
        s"non-empty-address count (supernode/common/total): ${utxosByErgoTreeHexSize.superNodeCount}/${utxosByErgoTreeHexSize.commonSize}/${utxosByErgoTreeHex.size} "
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

  def writeReportAndCompact(indexing: Boolean): Task[Unit] =
    ZIO.collectAllDiscard(
      getReportByPath.map { case (path, hotKeys) =>
        val newLines = hotKeys.collect { case NewHotKey(hotKey, SuperNodeCounter(writeOps, readOps, boxesAdded, boxesRemoved)) =>
          val stats  = s"$writeOps $readOps $boxesAdded $boxesRemoved ${boxesAdded - boxesRemoved}"
          val indent = 45
          s"$stats ${List.fill(Math.max(4, indent - stats.length))(" ").mkString("")} $hotKey"
        }
        ZIO.when(newLines.nonEmpty)(ZIO.log(s"New ${newLines.size} hotkeys $path")) *>
        SuperNodeCounter.writeReport(
          hotKeys.map(_.key),
          path
        )
      }
    ) *> compact(indexing)

  def commit(): Task[Revision] = ZIO.attempt(store.commit())

  def rollbackTo(rev: Revision): Task[Unit] = ZIO.attempt(store.rollbackTo(rev)) *> clearEmptySuperNodes

  def removeInputBoxesByErgoTree(inputIds: Seq[BoxId]): Task[_] = ZIO.fromTry {
    inputIds
      .flatMap { inputId =>
        ergoTreeHexByUtxo.get(inputId).map(_ -> inputId)
      }
      .groupBy(_._1)
      .foreach { case (et, inputBoxes) =>
        utxosByErgoTreeHex.removeAllOrFail(et, inputBoxes.iterator.map(_._2), inputBoxes.size) { existingBoxIds =>
          inputBoxes.iterator.foreach(t => existingBoxIds.remove(t._2))
          Option(existingBoxIds).collect { case m if !m.isEmpty => m }
        }
      }

    ergoTreeHexByUtxo
      .removeAllOrFail(inputIds)
  }

  def removeInputBoxesByErgoTreeT8(inputIds: Seq[BoxId]): Task[_] = ZIO.attempt {
    inputIds
      .flatMap { inputId =>
        ergoTreeT8HexByUtxo.get(inputId).map(_ -> inputId)
      }
      .groupBy(_._1)
      .foreach { case (etT8, inputBoxes) =>
        utxosByErgoTreeT8Hex
          .removeSubsetOrFail(etT8, inputBoxes.iterator.map(_._2), inputBoxes.size) { existingBoxIds =>
            inputBoxes.iterator.foreach(t => existingBoxIds.remove(t._2))
            Option(existingBoxIds).collect { case m if !m.isEmpty => m }
          }
      }

    ergoTreeT8HexByUtxo
      .removeAllExisting(inputIds)
  }

  def removeInputBoxesByTokenId(inputIds: Seq[BoxId]): Task[_] = ZIO.attempt {
    inputIds
      .flatMap { inputId =>
        val tokensOpt = tokensByUtxo.getAll(inputId)
        if (tokensOpt.exists(ts => !ts.isEmpty)) {
          val tokens = tokensOpt.toSeq.flatMap(_.keySet().asScala)
          tokensByUtxo
            .removeAllOrFail(inputId, tokens, tokens.size) { existingBoxIds =>
              tokens.iterator.foreach(t => existingBoxIds.remove(t))
              Option(existingBoxIds).collect { case m if !m.isEmpty => m }
            }
            .get
          tokens.map(_ -> inputId)
        } else
          Seq.empty
      }
      .groupBy(_._1)
      .foreach { case (tokenId, inputBoxes) =>
        utxosByTokenId
          .removeSubsetOrFail(tokenId, inputBoxes.map(_._2), inputBoxes.size) { existingBoxIds =>
            inputBoxes.iterator.foreach(t => existingBoxIds.remove(t._2))
            Option(existingBoxIds).collect { case m if !m.isEmpty => m }
          }
          .get
      }
  }

  // TODO parallel writes to 2 different NvMaps
  def persistErgoTreeByUtxo(byErgoTree: Iterable[(ErgoTree, mutable.Set[Utxo])]): Task[_] = ZIO.attempt {
    byErgoTree
      .foreach { case (ergoTreeHex, boxes) =>
        ergoTreeHexByUtxo
          .putAllNewOrFail(boxes.iterator.map(b => b.boxId -> ergoTreeHex.hex))
          .flatMap { _ =>
            utxosByErgoTreeHex.adjustAndForget(ergoTreeHex.hex, boxes.iterator.map(b => b.boxId -> b.ergValue), boxes.size)
          }
          .get
      }
  }

  def persistErgoTreeT8ByUtxo(byErgoTreeT8: Iterable[(ErgoTreeT8, mutable.Set[Utxo])]): Task[_] = ZIO.attempt {
    byErgoTreeT8
      .foreach { case (ergoTreeT8, boxes) =>
        ergoTreeT8HexByUtxo
          .putAllNewOrFail(boxes.iterator.map(b => b.boxId -> ergoTreeT8.hex))
          .flatMap { _ =>
            utxosByErgoTreeT8Hex.adjustAndForget(ergoTreeT8.hex, boxes.iterator.map(_.boxId), boxes.size)
          }
          .get
      }
  }

  def persistTokensByUtxo(assets: mutable.Map[BoxId, mutable.Map[TokenId, Amount]]): Task[_] = ZIO.attempt {
    assets.foreach { case (boxId, ammountByTokenId) =>
      tokensByUtxo.adjustAndForget(boxId, ammountByTokenId.iterator, ammountByTokenId.size).get
    }
  }

  def persistUtxosByTokenId(assets: mutable.Map[TokenId, mutable.Set[BoxId]]): Task[_] = ZIO.attempt {
    assets.foreach { case (tokenId, boxIds) =>
      utxosByTokenId.adjustAndForget(tokenId, boxIds.iterator, boxIds.size).get
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

  private val VersionsToKeep = 10

  def apply(homeDir: Path, mvStoreConf: MvStoreConf)(implicit store: MVStore): ZIO[Any, Throwable, MvStorage] =
    for
      utxosByErgoTreeHex   <- MultiMvMap[ErgoTreeHex, util.Map, BoxId, Value]("utxosByErgoTreeHex", homeDir)
      utxosByErgoTreeT8Hex <- MultiMvSet[ErgoTreeT8Hex, util.Set, BoxId]("utxosByErgoTreeT8Hex", homeDir)
      ergoTreeHexByUtxo    <- MvMap[BoxId, ErgoTreeHex]("ergoTreeHexByUtxo")
      ergoTreeT8HexByUtxo  <- MvMap[BoxId, ErgoTreeT8Hex]("ergoTreeT8HexByUtxo")
      blockIdsByHeight     <- MvMap[Height, util.Set[BlockId]]("blockIdsByHeight")
      blockById            <- MvMap[BlockId, Block]("blockById")
      utxosByTokenId       <- MultiMvSet[TokenId, util.Set, BoxId]("utxosByTokenId", homeDir)
      tokensByUtxo         <- MultiMvMap[BoxId, util.Map, TokenId, Amount]("tokensByUtxo", homeDir)
    yield MvStorage(
      utxosByErgoTreeHex,
      utxosByErgoTreeT8Hex,
      ergoTreeHexByUtxo,
      ergoTreeT8HexByUtxo,
      blockIdsByHeight,
      blockById,
      utxosByTokenId,
      tokensByUtxo
    )(store, mvStoreConf)

  def buildMvStore(dbFile: File, mvStoreConf: MvStoreConf): Task[MVStore] = ZIO
    .attempt {
      dbFile.getParentFile.mkdirs()
      implicit val store: MVStore =
        new MVStore.Builder()
          .fileName(dbFile.getAbsolutePath)
          .cacheSize(mvStoreConf.cacheSize)
          .autoCommitDisabled()
          .open()

      store.setVersionsToKeep(VersionsToKeep)
      store.setRetentionTime(3600 * 1000 * 24 * 7)
      store
    }

  private def zlayer(dbFile: File): ZLayer[MvStoreConf, Throwable, MvStorage] =
    ZLayer.service[MvStoreConf].flatMap { mvStoreConf =>
      ZLayer.scoped(
        ZIO.acquireRelease {
          buildMvStore(dbFile, mvStoreConf.get)
            .flatMap { implicit store =>
              ZIO.log(s"Building MvStorage at version ${store.getCurrentVersion}") *> MvStorage(dbFile.getParentFile.toPath, mvStoreConf.get)
            }
        } { storage =>
          ZIO.log(s"Closing mvstore at version ${storage.store.getCurrentVersion}") *> ZIO.succeed(storage.store.close())
        }
      )
    }

  def zlayerWithTempDirPerJvmRun: ZLayer[MvStoreConf, Throwable, MvStorage] = zlayer(
    tempDir.resolve(s"mv-store-$randomNumberPerJvmRun.db").toFile
  )

  def zlayerWithTempDirPerEachRun: ZLayer[MvStoreConf, Throwable, MvStorage] = zlayer(
    tempDir.resolve(s"mv-store-$randomNumberPerRun.db").toFile
  )

  def zlayerWithDefaultDir: ZLayer[MvStoreConf, Throwable, MvStorage] = zlayer(ergoHomeDir.resolve("mv-store.db").toFile)
}
