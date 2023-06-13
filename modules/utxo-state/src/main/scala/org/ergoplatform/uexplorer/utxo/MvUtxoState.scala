package org.ergoplatform.uexplorer.utxo

import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferInput, ByteBufferOutput, Input, Output}
import com.esotericsoftware.kryo.serializers.MapSerializer
import com.esotericsoftware.kryo.util.Pool
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.shaded.kryo.pool.KryoPool
import org.ergoplatform.uexplorer.{Address, *}
import org.ergoplatform.uexplorer.db.{BestBlockInserted, Block, BlockBuilder, ForkInserted, MvUeMap}
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ApiTransaction}
import org.ergoplatform.uexplorer.utxo.MvUtxoState.*
import org.h2.mvstore.{MVMap, MVStore}
import KryoSerialization.Implicits.*
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

case class MvUtxoState(
  store: MVStore,
  utxosByAddress: MvUeMap[Address, java.util.Map[BoxId, Value]],
  addressByUtxo: MvUeMap[BoxId, Address],
  topAddresses: MvUeMap[Address, Address.Stats],
  blockIdsByHeight: MvUeMap[Height, java.util.Set[BlockId]],
  blockById: MvUeMap[BlockId, BlockMetadata]
) extends UtxoState
  with LazyLogging {

  private def persistNewBlock(blockId: BlockId, height: Height, block: BlockMetadata): Try[Unit] = Try {
    blockById.put(blockId, block)
    blockIdsByHeight.adjust(height)(
      _.fold(javaSetOf(blockId)) { existingBlockIds =>
        existingBlockIds.add(blockId)
        existingBlockIds
      }
    )
    ()
  }

  private def getBlocksByHeight(atHeight: Height): Map[BlockId, BlockMetadata] =
    blockIdsByHeight
      .get(atHeight)
      .map(_.asScala.flatMap(blockId => blockById.get(blockId).map(blockId -> _)).toMap)
      .getOrElse(Map.empty)

  def getUtxosByAddress(address: Address): Option[Map[BoxId, Value]] =
    utxosByAddress.get(address).map(_.asScala.toMap)

  def isEmpty: Boolean =
    utxosByAddress.isEmpty && addressByUtxo.isEmpty && topAddresses.isEmpty && blockIdsByHeight.isEmpty && blockById.isEmpty

  def getLastHeight: Option[Height] = blockIdsByHeight.lastKey

  def getLastBlocks: Map[BlockId, BlockMetadata] =
    blockIdsByHeight.lastKey
      .map { lastHeight =>
        getBlocksByHeight(lastHeight)
      }
      .getOrElse(Map.empty)

  def getAddressStats(address: Address): Option[Address.Stats] = topAddresses.get(address)

  def containsBlock(blockId: BlockId, atHeight: Height): Boolean =
    blockById.get(blockId).exists(_.height == atHeight)

  def getAddressByUtxo(boxId: BoxId): Option[Address] = addressByUtxo.get(boxId)

  def findMissingHeights: TreeSet[Height] = {
    val lastHeight = getLastHeight
    if (lastHeight.isEmpty || lastHeight.contains(1))
      TreeSet.empty
    else
      TreeSet((1 to lastHeight.get): _*).diff(blockIdsByHeight.keySet.asScala)
  }

  private def mergeBlockBoxesUnsafe(
    block: Block,
    boxes: Iterator[(Iterable[(BoxId, Address, Value)], Iterable[(BoxId, Address, Value)])]
  ): Try[Unit] = Try {
    val currentVersion = store.getCurrentVersion
    val blockMetadata  = BlockMetadata.fromBlock(block, currentVersion)
    boxes.foreach { case (inputBoxes, outputBoxes) =>
      outputBoxes
        .foldLeft(mutable.Map.empty[Address, Int]) { case (acc, (boxId, address, value)) =>
          addressByUtxo.put(boxId, address)
          utxosByAddress.adjust(address)(_.fold(javaMapOf(boxId, value)) { arr =>
            arr.put(boxId, value)
            arr
          })
          acc.adjust(address)(_.fold(1)(_ + 1))
        }
        .foreach { case (address, boxCount) =>
          topAddresses.adjust(address) {
            case None =>
              Address.Stats(blockMetadata.height, 1, boxCount)
            case Some(Address.Stats(_, oldTxCount, oldBoxCount)) =>
              Address.Stats(blockMetadata.height, oldTxCount + 1, oldBoxCount + boxCount)
          }
        }
      inputBoxes
        .groupBy(_._2)
        .view
        .mapValues(_.map(_._1))
        .foreach { case (address, inputIds) =>
          utxosByAddress.putOrRemove(address) {
            case None => None
            case Some(existingBoxIds) =>
              inputIds.foreach(existingBoxIds.remove)
              Option(existingBoxIds).collect { case m if !m.isEmpty => m }
          }
        }

      inputBoxes.foreach { i =>
        addressByUtxo.remove(i._1)
      }
    }
    block.header.id -> blockMetadata
  }.flatMap { case (blockId, blockMetadata) =>
    persistNewBlock(blockId, blockMetadata.height, blockMetadata).map { _ =>
      if (blockMetadata.height % (MvUtxoState.MaxCacheSize * 1000) == 0) {
        Try(store.compactFile(60000 * 10)) // 10 minutes
        logger.info(
          s"Compacting at height ${blockMetadata.height}, utxo count: ${addressByUtxo.size}, non-empty-address count: ${utxosByAddress.size}"
        )
      }
    }
  }

  /** Genesis block has no parent so we assert that any block either has its parent cached or its a first block */
  private def getParentOrFail(apiBlock: ApiFullBlock): Try[Option[BlockMetadata]] = {
    def fail =
      Failure(
        new IllegalStateException(
          s"Block ${apiBlock.header.id} at height ${apiBlock.header.height} has missing parent ${apiBlock.header.parentId}"
        )
      )
    if (apiBlock.header.height == 1)
      Try(Option.empty)
    else if (containsBlock(apiBlock.header.parentId, apiBlock.header.height - 1))
      blockById.get(apiBlock.header.parentId).fold(fail)(parent => Try(Option(parent)))
    else
      fail
  }

  def addBestBlock(apiFullBlock: ApiFullBlock)(implicit
    ps: ProtocolSettings
  ): Try[BestBlockInserted] =
    getParentOrFail(apiFullBlock)
      .flatMap { parentOpt =>
        BlockBuilder(apiFullBlock, parentOpt)
          .flatMap { b =>
            val outputLookup =
              apiFullBlock.transactions.transactions
                .flatMap(tx => tx.outputs.map(o => (o.boxId, (o.address, o.value))).toMap)
                .toMap

            val txs =
              apiFullBlock.transactions.transactions.zipWithIndex.map { case (tx, txIndex) =>
                val outputs = tx.outputs.map(o => (o.boxId, o.address, o.value))
                val inputs =
                  tx match {
                    case tx if tx.id == Const.Genesis.Emission.tx =>
                      ArraySeq(
                        (Const.Genesis.Emission.box, Const.Genesis.Emission.address, Const.Genesis.Emission.initialNanoErgs)
                      )
                    case tx if tx.id == Const.Genesis.Foundation.tx =>
                      ArraySeq(
                        (
                          Const.Genesis.Foundation.box,
                          Const.Genesis.Foundation.address,
                          Const.Genesis.Foundation.initialNanoErgs
                        )
                      )
                    case tx =>
                      tx.inputs.map { i =>
                        val valueByBoxId = outputLookup.get(i.boxId)
                        val inputAddress =
                          valueByBoxId
                            .map(_._1)
                            .orElse(addressByUtxo.get(i.boxId))
                            .getOrElse(
                              throw new IllegalStateException(
                                s"BoxId ${i.boxId} of block ${b.header.id} at height ${b.header.height} not found in utxo state" + outputs
                                  .mkString("\n", "\n", "\n")
                              )
                            )
                        val inputValue =
                          valueByBoxId
                            .map(_._2)
                            .orElse(getUtxosByAddress(inputAddress).flatMap(_.get(i.boxId)))
                            .getOrElse(
                              throw new IllegalStateException(
                                s"Address $inputAddress of block ${b.header.id} at height ${b.header.height} not found in utxo state" + outputs
                                  .mkString("\n", "\n", "\n")
                              )
                            )
                        (i.boxId, inputAddress, inputValue)
                      }
                  }
                Tx(tx.id, txIndex.toShort, b.header.height, b.header.timestamp) -> (inputs, outputs)
              }

            mergeBlockBoxesUnsafe(b, txs.iterator.map(_._2)).map(_ => BestBlockInserted(b, txs))
          }
      }

  private def hasParentAndIsChained(fork: List[ApiFullBlock]): Boolean =
    fork.size > 1 && containsBlock(fork.head.header.parentId, fork.head.header.height - 1) &&
      fork.sliding(2).forall {
        case first :: second :: Nil =>
          first.header.id == second.header.parentId
        case _ =>
          false
      }

  def addWinningFork(winningFork: List[ApiFullBlock])(implicit protocol: ProtocolSettings): Try[ForkInserted] =
    if (!hasParentAndIsChained(winningFork)) {
      Failure(
        new UnexpectedStateError(
          s"Inserting fork ${winningFork.map(_.header.id).mkString(",")} at height ${winningFork.map(_.header.height).mkString(",")} illegal"
        )
      )
    } else {
      val supersededBlocks =
        winningFork.flatMap(w => getBlocksByHeight(w.header.height).filter(_._1 != w.header.id)).toMap
      Try(store.rollbackTo(blockById.get(winningFork.head.header.id).map(_.parentVersion).get))
        .flatMap { _ =>
          winningFork
            .foldLeft(Try(ListBuffer.empty[BestBlockInserted])) {
              case (f @ Failure(_), _) =>
                f
              case (Success(insertedBlocksAcc), apiBlock) =>
                addBestBlock(apiBlock).map { insertedBlock =>
                  insertedBlocksAcc :+ insertedBlock
                }
            }
            .map { newBlocks =>
              ForkInserted(newBlocks.toList, supersededBlocks)
            }
        }
    }

}

object MvUtxoState extends LazyLogging {
  val VersionsToKeep = 10
  val MaxCacheSize   = 10

  def apply(
    rootDir: File = Paths.get(System.getProperty("java.io.tmpdir"), Random.nextString(10)).toFile
  )(implicit s: ActorSystem[Nothing]): Try[MvUtxoState] = Try {
    rootDir.mkdirs()
    val store =
      new MVStore.Builder()
        .fileName(rootDir.toPath.resolve("mvstore").toFile.getAbsolutePath)
        .autoCommitDisabled()
        .open()

    store.setVersionsToKeep(VersionsToKeep)
    store.setRetentionTime(3600 * 1000 * 24 * 7)

    val utxoState =
      MvUtxoState(
        store,
        new MvUeMap[Address, java.util.Map[BoxId, Value]]("utxosByAddress", store),
        new MvUeMap[BoxId, Address]("addressByUtxo", store),
        new MvUeMap[Address, Address.Stats]("topAddresses", store),
        new MvUeMap[Height, java.util.Set[BlockId]]("blockIdsByHeight", store),
        new MvUeMap[BlockId, BlockMetadata]("blockById", store)
      )
    CoordinatedShutdown(s).addTask(
      CoordinatedShutdown.PhaseServiceStop,
      "close-mv-store"
    ) { () =>
      Future(store.close()).map(_ => Done)
    }
    utxoState
  }

  def withDefaultDir()(implicit s: ActorSystem[Nothing]): Try[MvUtxoState] =
    MvUtxoState(Paths.get(System.getProperty("user.home"), ".ergo-uexplorer", "utxo").toFile)
}
