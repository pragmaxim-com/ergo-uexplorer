package org.ergoplatform.uexplorer.utxo

import akka.actor.CoordinatedShutdown
import akka.{Done, NotUsed}
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.Source
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferInput, ByteBufferOutput, Input, Output}
import com.esotericsoftware.kryo.serializers.MapSerializer
import com.esotericsoftware.kryo.util.Pool
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.shaded.kryo.pool.KryoPool
import org.ergoplatform.uexplorer.db.{BestBlockInserted, Block, BlockBuilder, ForkInserted}
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ApiTransaction}
import org.ergoplatform.uexplorer.utxo.MvUtxoState.*
import org.ergoplatform.uexplorer.*
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

class MvUtxoState(
  store: MVStore,
  utxosByAddress: MVMap[Address, Array[Byte]],
  addressByUtxo: MVMap[BoxId, Address],
  topAddresses: MVMap[Address, Address.Stats],
  blockIdByHeight: MVMap[Height, BlockId],
  blockCacheByHeight: ConcurrentSkipListMap[Height, Map[BlockId, BlockMetadata]]
) extends UtxoState
  with LazyLogging {

  private val kryoPool = new Pool[Kryo](true, false, 8) {
    protected def create: Kryo = {
      val kryo       = new Kryo()
      val serializer = new MapSerializer()
      kryo.register(classOf[util.HashMap[_, _]], serializer)
      serializer.setKeyClass(classOf[String], kryo.getSerializer(classOf[String]))
      serializer.setKeysCanBeNull(false)
      serializer.setValueClass(classOf[java.lang.Long], kryo.getSerializer(classOf[java.lang.Long]))
      serializer.setValuesCanBeNull(false)
      kryo
    }
  }

  private def serValueByBoxId(valueByBoxId: java.util.Map[String, Long]): Array[Byte] = {
    val buffer = ByteBuffer.allocate(valueByBoxId.size() * 256)
    val output = new ByteBufferOutput(buffer)
    val kryo   = kryoPool.obtain()
    try kryo.writeObject(output, valueByBoxId)
    finally {
      kryoPool.free(kryo)
      output.close()
    }
    buffer.array()
  }

  private def serEmpty(boxId: BoxId, value: Value): Array[Byte] = {
    val map = new util.HashMap[String, Long]()
    map.put(boxId.unwrapped, value)
    serValueByBoxId(map)
  }

  private def deserValueByBoxId(bytes: Array[Byte]): java.util.Map[String, Long] = {
    val input = new Input(bytes)
    val kryo  = kryoPool.obtain()
    try kryo.readObject(input, classOf[util.HashMap[String, Long]])
    finally {
      kryoPool.free(kryo)
      input.close()
    }
  }

  private def addValue(bytes: Array[Byte], boxId: BoxId, value: Value): Array[Byte] = {
    val map = deserValueByBoxId(bytes)
    map.put(boxId.unwrapped, value)
    serValueByBoxId(map)
  }

  private[this] def cacheBlock(
    lastHeight: Height,
    lastBlockId: BlockId,
    blockMetadata: BlockMetadata
  ): Map[BlockId, BlockMetadata] =
    blockCacheByHeight.put(lastHeight, Map(lastBlockId -> blockMetadata))

  def isEmpty: Boolean = utxosByAddress.isEmpty && addressByUtxo.isEmpty && topAddresses.isEmpty && blockIdByHeight.isEmpty

  def getLastBlock: Option[(Height, BlockId)] = Option(blockIdByHeight.lastKey()).map { lastKey =>
    lastKey -> blockIdByHeight.get(lastKey)
  }

  def getFirstBlock: Option[(Height, BlockId)] = Option(blockIdByHeight.firstKey()).map { firstKey =>
    firstKey -> blockIdByHeight.get(firstKey)
  }

  def utxoBoxCount: Int = addressByUtxo.size()

  def nonEmptyAddressCount: Int = utxosByAddress.size()

  def getAddressStats(address: Address): Option[Address.Stats] = Option(topAddresses.get(address))

  def containsBlock(blockId: BlockId, atHeight: Height): Boolean = blockIdByHeight.get(atHeight) == blockId

  def getAddressByUtxo(boxId: BoxId): Option[Address] = Option(addressByUtxo.get(boxId))

  def getUtxosByAddress(address: Address): Option[Map[BoxId, Value]] = Option(
    deserValueByBoxId(utxosByAddress.get(address)).asScala.toMap.asInstanceOf[Map[BoxId, Value]]
  )

  def getTopAddresses: Iterator[(Address, Address.Stats)] = new Iterator[(Address, Address.Stats)]() {
    private val cursor = topAddresses.cursor(null.asInstanceOf[Address])

    override def hasNext: Boolean = cursor.hasNext

    override def next(): (Address, Address.Stats) = cursor.next() -> cursor.getValue
  }

  def getBlocksByHeight: Iterator[(Height, BlockId)] = new Iterator[(Height, BlockId)]() {
    private val cursor = blockIdByHeight.cursor(1)

    override def hasNext: Boolean = cursor.hasNext

    override def next(): (Height, BlockId) = cursor.next() -> cursor.getValue
  }

  def findMissingHeights: TreeSet[Height] = {
    val lastBlock = getLastBlock
    if (lastBlock.isEmpty || lastBlock.map(_._1).contains(1))
      TreeSet.empty
    else
      TreeSet((getFirstBlock.get._1 to lastBlock.get._1): _*)
        .diff(blockIdByHeight.keySet().asScala)
  }

  def compactFile(maxCompactTimeMillis: Int): Try[Unit] = Try(store.compactFile(maxCompactTimeMillis))

  def commit(): Long = store.commit()

  def rolbackToLatest(): Option[(Height, BlockId)] =
    Option(blockIdByHeight.lastKey())
      .map(_ - 1)
      .filter(_ > 0)
      .map { previousHeight =>
        val previousBlockId = blockIdByHeight.get(previousHeight)
        logger.info(s"Rolling back to version/height $previousHeight with block $previousBlockId")
        store.rollbackTo(previousHeight)
        previousHeight -> previousBlockId
      }

  def flushCache(): Unit =
    blockCacheByHeight
      .keySet()
      .iterator()
      .asScala
      .take(Math.max(0, blockCacheByHeight.size() - MaxCacheSize))
      .foreach(blockCacheByHeight.remove)

  def mergeBlockBoxesUnsafe(
    height: Height,
    blockId: BlockId,
    boxes: Iterator[(Iterable[(BoxId, Address, Value)], Iterable[(BoxId, Address, Value)])]
  ): Try[Unit] = Try {
    boxes.foreach { case (inputBoxes, outputBoxes) =>
      outputBoxes
        .foldLeft(mutable.Map.empty[Address, Int]) { case (acc, (boxId, address, value)) =>
          addressByUtxo.put(boxId, address)
          utxosByAddress.adjust(address)(
            _.fold(serEmpty(boxId, value))(arr => addValue(arr, boxId, value))
          )
          acc.adjust(address)(_.fold(1)(_ + 1))
        }
        .foreach { case (address, boxCount) =>
          topAddresses.adjust(address) {
            case None =>
              Address.Stats(height, 1, boxCount)
            case Some(Address.Stats(_, oldTxCount, oldBoxCount)) =>
              Address.Stats(height, oldTxCount + 1, oldBoxCount + boxCount)
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
    blockIdByHeight.put(height, blockId)
    require(store.commit() == height, s"Height $height")
  }

  private def getParentOrFail(apiBlock: ApiFullBlock): Try[Option[BlockMetadata]] =
    /** Genesis block has no parent so we assert that any block either has its parent cached or its a first block */
    if (apiBlock.header.height == 1)
      Try(Option.empty)
    else
      Option(blockCacheByHeight.get(apiBlock.header.height - 1))
        .flatMap(_.get(apiBlock.header.parentId))
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
          .map { b =>
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
                            .orElse(Option(deserValueByBoxId(utxosByAddress.get(inputAddress)).get(i.boxId)))
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

            mergeBlockBoxesUnsafe(b.header.height, b.header.id, txs.iterator.map(_._2))
            blockCacheByHeight.adjust(b.header.height)(
              _.fold(Map(b.header.id -> BlockMetadata.fromBlock(b)))(
                _.updated(b.header.id, BlockMetadata.fromBlock(b))
              )
            )
            BestBlockInserted(b, txs)
          }
      }

  private def hasParentAndIsChained(fork: List[ApiFullBlock]): Boolean =
    fork.size > 1 &&
      blockCacheByHeight.get(fork.head.header.height - 1).contains(fork.head.header.parentId) &&
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
      Try(store.rollbackTo(winningFork.head.header.height - 1))
        .flatMap { _ =>
          winningFork
            .foldLeft(Try((ListBuffer.empty[BestBlockInserted], ListBuffer.empty[BlockMetadata]))) {
              case (f @ Failure(_), _) =>
                f
              case (Success((insertedBlocksAcc, toRemoveAcc)), apiBlock) =>
                addBestBlock(apiBlock).map { insertedBlock =>
                  val toRemove =
                    toRemoveAcc ++ blockCacheByHeight
                      .get(apiBlock.header.height)
                      .filter(_._1 != apiBlock.header.id)
                      .values
                  (insertedBlocksAcc :+ insertedBlock, toRemove)
                }
            }
            .map { case (newBlocks, supersededBlocks) =>
              ForkInserted(newBlocks.toList, supersededBlocks.toList)
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
      new MvUtxoState(
        store,
        store.openMap[Address, Array[Byte]]("utxosByAddress"),
        store.openMap[BoxId, Address]("addressByUtxo"),
        store.openMap[Address, Address.Stats]("topAddresses"),
        store.openMap[Height, BlockId]("blockIdByHeight"),
        new ConcurrentSkipListMap[Height, Map[BlockId, BlockMetadata]]()
      )
    CoordinatedShutdown(s).addTask(
      CoordinatedShutdown.PhaseServiceStop,
      "close-mv-store"
    ) { () =>
      Future(store.close()).map(_ => Done)
    }
    utxoState
  }
  def getLastMetadataRecursively(
    lastHeight: Height,
    lastBlockId: BlockId,
    utxoState: MvUtxoState,
    getLastBlockMetadata: BlockId => Future[Option[BlockMetadata]]
  ): Future[Option[BlockMetadata]] = {
    logger.info(s"Getting last block metadata for height/version $lastHeight with block $lastBlockId")
    getLastBlockMetadata(lastBlockId).flatMap {
      case Some(blockMetadata) =>
        utxoState.cacheBlock(lastHeight, lastBlockId, blockMetadata)
        Future.successful(Some(blockMetadata))
      case None =>
        utxoState.rolbackToLatest() match {
          case Some(prevHeight, prevBlockId) =>
            getLastMetadataRecursively(prevHeight, prevBlockId, utxoState, getLastBlockMetadata)
          case None =>
            Future.successful(None)
        }
    }
  }

  def withCache(
    getLastBlockMetadata: BlockId => Future[Option[BlockMetadata]],
    rootDir: File = Paths.get(System.getProperty("user.home"), ".ergo-uexplorer", "utxo").toFile
  )(implicit s: ActorSystem[Nothing]): Future[MvUtxoState] =
    MvUtxoState(rootDir) match {
      case Success(utxoState) if !utxoState.isEmpty =>
        val (lastHeight, lastBlockId) = utxoState.getLastBlock.get
        getLastMetadataRecursively(lastHeight, lastBlockId, utxoState, getLastBlockMetadata).flatMap {
          case Some(_) =>
            logger.info(s"Found metadata for height/version $lastHeight with block $lastBlockId")
            Future.successful(utxoState)
          case None =>
            Future.failed(
              new IllegalStateException(s"last block $lastBlockId at $lastHeight does not have metadata in backend!")
            )
        }
      case Success(utxoState) =>
        Future.successful(utxoState)
      case Failure(ex) =>
        Future.failed(ex)
    }

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
