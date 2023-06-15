package org.ergoplatform.uexplorer.indexer.chain

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import org.ergoplatform.uexplorer.db.*
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

class BlockIndexer(storage: MvStorage, backendEnabled: Boolean) extends LazyLogging {

  def readableStorage: Storage = storage

  private def mergeBlockBoxesUnsafe(block: LightBlock): Try[Unit] = Try {
    val boxes          = block.boxesByTx.iterator.map(_._2)
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
    block.headerId -> blockMetadata
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

  def addBestBlocks(winningFork: List[ApiFullBlock])(implicit ps: ProtocolSettings): Try[ListBuffer[BestBlockInserted]] =
    winningFork
      .foldLeft(Try(ListBuffer.empty[BestBlockInserted])) {
        case (f @ Failure(_), _) =>
          f
        case (Success(insertedBlocksAcc), apiBlock) =>
          addBestBlock(apiBlock).map { insertedBlock =>
            insertedBlocksAcc :+ insertedBlock
          }
      }

  def addBestBlock(apiFullBlock: ApiFullBlock)(implicit ps: ProtocolSettings): Try[BestBlockInserted] =
    for {
      parentOpt <- getParentOrFail(apiFullBlock)
      lb        <- LightBlockBuilder(apiFullBlock, parentOpt, storage.getAddressByUtxo, storage.getUtxosByAddress)
      fb        <- if (backendEnabled) FullBlockBuilder(apiFullBlock, parentOpt).map(Some(_)) else Try(None)
      _         <- mergeBlockBoxesUnsafe(lb)
      _         <- storage.compact(lb.height)
    } yield BestBlockInserted(lb, fb)

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
      logger.info(s"Adding fork from height ${winningFork.head.header.height} until ${winningFork.last.header.height}")
      for {
        preForkVersion <- Try(storage.getBlockById(winningFork.head.header.id).map(_.parentVersion).get)
        toRemove = winningFork.flatMap(b => storage.getBlocksByHeight(b.header.height).filter(_._1 != b.header.id)).toMap
        _         <- storage.rollbackTo(preForkVersion)
        newBlocks <- addBestBlocks(winningFork)
      } yield ForkInserted(
        newBlocks.toList,
        toRemove
      )
    }
}

object BlockIndexer {
  def apply(storage: MvStorage, backendEnabled: Boolean)(implicit system: ActorSystem[Nothing]): BlockIndexer = {
    CoordinatedShutdown(system).addTask(
      CoordinatedShutdown.PhaseServiceStop,
      "close-mv-store"
    ) { () =>
      Future(storage.close()).map(_ => Done)
    }
    new BlockIndexer(storage, backendEnabled)
  }
}
