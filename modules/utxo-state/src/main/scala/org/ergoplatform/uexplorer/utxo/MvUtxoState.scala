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
import org.ergoplatform.uexplorer.db.{BestBlockInserted, Block, BlockBuilder, ForkInserted}
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ApiTransaction}
import org.ergoplatform.uexplorer.utxo.MvUtxoState.*
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

case class MvUtxoState(
  store: MVStore,
  utxosByAddress: MVMap[Address, Array[Byte]],
  addressByUtxo: MVMap[BoxId, Address],
  topAddresses: MVMap[Address, Array[Byte]],
  blockByHeight: MVMap[Height, Array[Byte]]
) extends UtxoState
  with KryoSerialization
  with LazyLogging {

  private def putBlockByHeight(height: Height, block: BlockMetadata): Try[Unit] =
    serBlock(block).map { b =>
      blockByHeight.put(height, b)
    }

  private def getBlockByHeight(height: Height): Option[BlockMetadata] =
    Option(blockByHeight.get(height)).map(deserBlock)

  private def getTopAddress(address: Address): Option[Address.Stats] =
    Option(topAddresses.get(address)).map(deserStats)

  def getUtxosByAddress(address: Address): Option[Map[BoxId, Value]] =
    Option(utxosByAddress.get(address)).map(m => deserValueByBoxId(m).asScala.toMap)

  def isEmpty: Boolean = utxosByAddress.isEmpty && addressByUtxo.isEmpty && topAddresses.isEmpty && blockByHeight.isEmpty

  def getLastBlock: Option[(Height, BlockMetadata)] = Option(blockByHeight.lastKey()).map { lastKey =>
    lastKey -> getBlockByHeight(lastKey).get
  }

  def getFirstBlock: Option[(Height, BlockMetadata)] = Option(blockByHeight.firstKey()).map { firstKey =>
    firstKey -> getBlockByHeight(firstKey).get
  }

  def utxoBoxCount: Int = addressByUtxo.size()

  def nonEmptyAddressCount: Int = utxosByAddress.size()

  def getAddressStats(address: Address): Option[Address.Stats] = getTopAddress(address)

  def containsBlock(blockId: BlockId, atHeight: Height): Boolean =
    getBlockByHeight(atHeight).exists(_.headerId == blockId)

  def getAddressByUtxo(boxId: BoxId): Option[Address] = Option(addressByUtxo.get(boxId))

  def getTopAddresses: Iterator[(Address, Address.Stats)] = new Iterator[(Address, Address.Stats)]() {
    private val cursor = topAddresses.cursor(null.asInstanceOf[Address])

    override def hasNext: Boolean = cursor.hasNext

    override def next(): (Address, Address.Stats) = cursor.next() -> deserStats(cursor.getValue)
  }

  def getBlocksByHeight: Iterator[(Height, BlockMetadata)] = new Iterator[(Height, BlockMetadata)]() {
    private val cursor = blockByHeight.cursor(1)

    override def hasNext: Boolean = cursor.hasNext

    override def next(): (Height, BlockMetadata) = cursor.next() -> deserBlock(cursor.getValue)
  }

  def findMissingHeights: TreeSet[Height] = {
    val lastBlock = getLastBlock
    if (lastBlock.isEmpty || lastBlock.map(_._1).contains(1))
      TreeSet.empty
    else
      TreeSet((getFirstBlock.get._1 to lastBlock.get._1): _*)
        .diff(blockByHeight.keySet().asScala)
  }

  def compactFile(maxCompactTimeMillis: Int): Try[Unit] = Try(store.compactFile(maxCompactTimeMillis))

  def commit(): Long = store.commit()

  private def mergeBlockBoxesUnsafe(
    block: Block,
    boxes: Iterator[(Iterable[(BoxId, Address, Value)], Iterable[(BoxId, Address, Value)])]
  ): Try[Unit] = Try {
    val blockMetadata = BlockMetadata.fromBlock(block)
    boxes.foreach { case (inputBoxes, outputBoxes) =>
      outputBoxes
        .foldLeft(mutable.Map.empty[Address, Int]) { case (acc, (boxId, address, value)) =>
          addressByUtxo.put(boxId, address)
          utxosByAddress.adjust(address)(
            _.fold(serValueByBoxIdNew(boxId, value))(arr => addValue(arr, boxId, value))
          )
          acc.adjust(address)(_.fold(1)(_ + 1))
        }
        .foreach { case (address, boxCount) =>
          topAddresses.adjust(address) {
            case None =>
              serStats(Address.Stats(blockMetadata.height, 1, boxCount))
            case Some(bytes) =>
              val Address.Stats(_, oldTxCount, oldBoxCount) = deserStats(bytes)
              serStats(Address.Stats(blockMetadata.height, oldTxCount + 1, oldBoxCount + boxCount))
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
              val map = deserValueByBoxId(existingBoxIds)
              inputIds.foreach(map.remove)
              Option(map).collect { case m if !m.isEmpty => serValueByBoxId(m) }
          }
        }

      inputBoxes.foreach { i =>
        addressByUtxo.remove(i._1)
      }
    }
    blockMetadata
  }.flatMap { blockMetadata =>
    putBlockByHeight(blockMetadata.height, blockMetadata).map { _ =>
      require(store.commit() == blockMetadata.height, s"Height ${blockMetadata.height}")
      if (blockMetadata.height % (MvUtxoState.MaxCacheSize * 1000) == 0) {
        compactFile(60000 * 10) // 10 minutes
        logger.info(
          s"Compacting at height ${blockMetadata.height}, utxo count: $utxoBoxCount, non-empty-address count: $nonEmptyAddressCount"
        )
      }
    }
  }

  private def getParentOrFail(apiBlock: ApiFullBlock): Try[Option[BlockMetadata]] =
    /** Genesis block has no parent so we assert that any block either has its parent cached or its a first block */
    if (apiBlock.header.height == 1)
      Try(Option.empty)
    else
      getBlockByHeight(apiBlock.header.height - 1)
        .fold(
          Failure(
            new IllegalStateException(
              s"Block ${apiBlock.header.id} at height ${apiBlock.header.height} has missing parent ${apiBlock.header.parentId}"
            )
          )
        )(parent => Try(Option(parent)))

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
                            .orElse(Option(addressByUtxo.get(i.boxId)))
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
    fork.size > 1 &&
      getBlockByHeight(fork.head.header.height - 1).exists(_.headerId == fork.head.header.parentId) &&
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
        winningFork.map(_.header.height).map(h => getBlockByHeight(h).get) // todo check there is no None
      Try(store.rollbackTo(winningFork.head.header.height - 1))
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
        store.openMap[Address, Array[Byte]]("utxosByAddress"),
        store.openMap[BoxId, Address]("addressByUtxo"),
        store.openMap[Address, Array[Byte]]("topAddresses"),
        store.openMap[Height, Array[Byte]]("blockByHeight")
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

  implicit class MVMapPimp[K, V](underlying: MVMap[K, V]) {

    def putOrRemove(k: K)(f: Option[V] => Option[V]): MVMap[K, V] =
      f(Option(underlying.get(k))) match {
        case None =>
          underlying.remove(k)
          underlying
        case Some(v) =>
          underlying.put(k, v)
          underlying
      }

    def adjust(k: K)(f: Option[V] => V): MVMap[K, V] = {
      underlying.put(k, f(Option(underlying.get(k))))
      underlying
    }

  }
  implicit class ConcurrentMapPimp[K, V](underlying: ConcurrentSkipListMap[K, V]) {

    def adjust(k: K)(f: Option[V] => V): ConcurrentSkipListMap[K, V] = {
      underlying.put(k, f(Option(underlying.get(k))))
      underlying
    }

  }

}
