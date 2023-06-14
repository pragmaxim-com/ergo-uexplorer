package org.ergoplatform.uexplorer.indexer.chain

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import org.ergoplatform.uexplorer.db.{BestBlockInserted, Block, BlockBuilder, ForkInserted}
import org.ergoplatform.uexplorer.mvstore.MvStorage
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
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.Const.Genesis.{Emission, Foundation}
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

class BlockIndexer(storage: MvStorage) extends LazyLogging {

  def readableStorage: Storage = storage

  private def mergeBlockBoxesUnsafe(
    block: Block,
    boxes: Iterator[(Iterable[(BoxId, Address, Value)], Iterable[(BoxId, Address, Value)])]
  ): Try[Unit] = Try {
    val currentVersion = storage.getCurrentVersion
    val blockMetadata  = BlockMetadata.fromBlock(block, currentVersion)
    boxes.foreach { case (inputBoxes, outputBoxes) =>
      outputBoxes
        .foreach { case (boxId, address, value) =>
          storage.persistBox(boxId, address, value)
        }
      inputBoxes
        .groupBy(_._2)
        .view
        .mapValues(_.collect { case (boxId, _, _) if boxId != Emission.box && boxId != Foundation.box => boxId })
        .foreach { case (address, inputIds) =>
          storage.removeInputBoxesByAddress(address, inputIds).get
        }
    }
    block.header.id -> blockMetadata
  }.flatMap { case (blockId, blockMetadata) =>
    storage.persistNewBlock(blockId, blockMetadata.height, blockMetadata)
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
    else if (storage.containsBlock(apiBlock.header.parentId, apiBlock.header.height - 1))
      storage.getBlockById(apiBlock.header.parentId).fold(fail)(parent => Try(Option(parent)))
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
                .flatMap(tx => tx.outputs.map(o => (o.boxId, (o.address, o.value))))
                .toMap

            val txs =
              apiFullBlock.transactions.transactions.zipWithIndex.map { case (tx, txIndex) =>
                val outputs = tx.outputs.map(o => (o.boxId, o.address, o.value))
                val inputs =
                  tx match {
                    case tx if tx.id == Const.Genesis.Emission.tx =>
                      ArraySeq((Emission.box, Emission.address, Emission.initialNanoErgs))
                    case tx if tx.id == Const.Genesis.Foundation.tx =>
                      ArraySeq((Foundation.box, Foundation.address, Foundation.initialNanoErgs))
                    case tx =>
                      tx.inputs.map { i =>
                        val valueByBoxId = outputLookup.get(i.boxId)
                        val inputAddress =
                          valueByBoxId
                            .map(_._1)
                            .orElse(storage.getAddressByUtxo(i.boxId))
                            .getOrElse(
                              throw new IllegalStateException(
                                s"BoxId ${i.boxId} of block ${b.header.id} at height ${b.header.height} not found in utxo state" + outputs
                                  .mkString("\n", "\n", "\n")
                              )
                            )
                        val inputValue =
                          valueByBoxId
                            .map(_._2)
                            .orElse(storage.getUtxosByAddress(inputAddress).flatMap(_.get(i.boxId)))
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

            mergeBlockBoxesUnsafe(b, txs.iterator.map(_._2)).flatMap { _ =>
              val blockInserted = BestBlockInserted(b, txs)
              if (b.header.height % MvStorage.CompactFileRate == 0) {
                logger.info(storage.getReport)
                storage.compact(b.header.height % MvStorage.MoveChunksRate == 0).map(_ => blockInserted)
              } else Success(blockInserted)
            }
          }
      }

  private def hasParentAndIsChained(fork: List[ApiFullBlock]): Boolean =
    fork.size > 1 && storage.containsBlock(fork.head.header.parentId, fork.head.header.height - 1) &&
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
        winningFork.flatMap(w => storage.getBlocksByHeight(w.header.height).filter(_._1 != w.header.id)).toMap
      Try(storage.getBlockById(winningFork.head.header.id).map(_.parentVersion).get)
        .flatMap { preForkVersion =>
          storage
            .rollbackTo(preForkVersion)
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

}

object BlockIndexer {
  def apply(storage: MvStorage)(implicit system: ActorSystem[Nothing]): BlockIndexer = {
    CoordinatedShutdown(system).addTask(
      CoordinatedShutdown.PhaseServiceStop,
      "close-mv-store"
    ) { () =>
      Future(storage.close()).map(_ => Done)
    }
    new BlockIndexer(storage)
  }
}
