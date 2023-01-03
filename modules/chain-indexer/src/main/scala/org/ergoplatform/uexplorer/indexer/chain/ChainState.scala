package org.ergoplatform.uexplorer.indexer.chain

import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.db.{Block, BlockInfo}
import org.ergoplatform.uexplorer.indexer.api.Backend
import org.ergoplatform.uexplorer.indexer.chain.ChainState.BlockBuffer
import org.ergoplatform.uexplorer.indexer.chain.ChainStateHolder.*
import org.ergoplatform.uexplorer.indexer.config.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.db.BlockBuilder
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState
import org.ergoplatform.uexplorer.indexer.{MapPimp, UnexpectedStateError}
import org.ergoplatform.uexplorer.node.ApiFullBlock

import scala.collection.immutable.{ArraySeq, List, SortedMap, SortedSet, TreeMap, TreeSet}
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

case class ChainState(
  lastBlockIdInEpoch: SortedMap[Int, BlockId],
  blockBuffer: BlockBuffer,
  inputsByHeightBuffer: Map[Int, Map[BoxId, (Address, Long)]],
  boxesByHeightBuffer: TreeMap[Int, ArraySeq[(TxId, (ArraySeq[(BoxId, Address, Long)], ArraySeq[(BoxId, Address, Long)]))]],
  utxoState: UtxoState
) {
  import ChainState.*

  /** on-demand computation of UtxoState up to latest blocks which are not merged right away as we do not support rollback,
    * instead we merge only winner-fork blocks into UtxoState
    */
  def utxoStateWithCurrentEpochBoxes: UtxoState =
    utxoState.mergeBoxes(boxesByHeightBuffer.iterator.flatMap(_._2.iterator.map(_._2)))

  def finishEpoch(currentEpochIndex: Int): Try[(MaybeNewEpoch, ChainState)] =
    if (lastBlockIdInEpoch.contains(currentEpochIndex)) {
      Success(NewEpochExisted(currentEpochIndex) -> this)
    } else {
      val previousEpochIndex = currentEpochIndex - 1
      val heightRange        = Epoch.heightRangeForEpochIndex(currentEpochIndex)
      EpochCandidate(blockBuffer.blockRelationsByHeight(heightRange)) match {
        case Right(candidate)
            if currentEpochIndex == 0 || lastBlockIdInEpoch(previousEpochIndex) == candidate.relsByHeight.head._2.parentId =>
          val newEpoch           = candidate.getEpoch
          val boxesByHeightSlice = boxesByHeightBuffer.range(heightRange.head, heightRange.last + 1)
          Try(utxoState.mergeBoxes(boxesByHeightSlice.iterator.flatMap(_._2.iterator.map(_._2))))
            .map { newState =>
              NewEpochDetected(newEpoch, boxesByHeightSlice) -> ChainState(
                lastBlockIdInEpoch.updated(newEpoch.index, newEpoch.blockIds.last),
                blockBuffer.flushEpoch(heightRange),
                inputsByHeightBuffer -- boxesByHeightSlice.keysIterator,
                boxesByHeightBuffer -- boxesByHeightSlice.keysIterator,
                newState
              )
            }
        case Right(candidate) =>
          val Epoch(curIndex, _) = candidate.getEpoch
          val invalidHeights =
            TreeSet(Epoch.heightRangeForEpochIndex(previousEpochIndex).last, candidate.relsByHeight.head._1)
          val error =
            s"Prev epoch $previousEpochIndex header ${lastBlockIdInEpoch.get(previousEpochIndex)} " +
            s"does not match current epoch $curIndex header, invalid heights: ${invalidHeights.mkString(",")}"
          Failure(new IllegalStateException(error))
        case Left(candidate) =>
          Failure(new IllegalStateException(candidate.error))
      }
    }

  def insertBestBlock(
    bestBlock: ApiFullBlock
  )(implicit protocol: ProtocolSettings): Try[(BestBlockInserted, ChainState)] =
    if (!hasParent(bestBlock)) {
      Failure(
        new UnexpectedStateError(
          s"Inserting block ${bestBlock.header.id} at ${bestBlock.header.height} without parent being applied is not possible"
        )
      )
    } else
      BlockBuilder(bestBlock, blockBuffer.byId.get(bestBlock.header.parentId))
        .map { b =>
          val newInputsByHeight = inputsByHeightBuffer.updated(
            b.header.height,
            bestBlock.transactions.transactions
              .flatMap(tx => tx.outputs.map(o => (o.boxId, (o.address, o.value))).toMap)
              .toMap
          )
          BestBlockInserted(b) -> copy(
            blockBuffer = blockBuffer.addBlock(b),
            newInputsByHeight,
            boxesByHeightBuffer = boxesByHeightBuffer.updated(
              b.header.height,
              bestBlock.transactions.transactions.map { tx =>
                val inputs = tx.inputs.map {
                  case i if i.boxId == Const.Genesis.Emission.box =>
                    (i.boxId, Const.Genesis.Emission.address, Const.Genesis.Emission.initialNanoErgs)
                  case i if i.boxId == Const.Genesis.NoPremine.box =>
                    (i.boxId, Const.Genesis.NoPremine.address, Const.Genesis.NoPremine.initialNanoErgs)
                  case i if i.boxId == Const.Genesis.Foundation.box =>
                    (i.boxId, Const.Genesis.Foundation.address, Const.Genesis.Foundation.initialNanoErgs)
                  case i =>
                    utxoState.addressByUtxo
                      .get(i.boxId)
                      .map(oAddr => (i.boxId, oAddr, utxoState.utxosByAddress(oAddr)(i.boxId)))
                      .getOrElse(
                        newInputsByHeight.valuesIterator
                          .collectFirst {
                            case xs if xs.contains(i.boxId) =>
                              val (a, v) = xs(i.boxId)
                              (i.boxId, a, v)
                          }
                          .getOrElse(throw IllegalStateException(s"Box ${i.boxId} in block ${b.header.id} cannot be found"))
                      )
                }
                tx.id -> (inputs, tx.outputs.map(o => (o.boxId, o.address, o.value)))
              }
            )
          )
        }

  def insertWinningFork(
    winningFork: List[ApiFullBlock]
  )(implicit protocol: ProtocolSettings): Try[(ForkInserted, ChainState)] =
    if (!hasParentAndIsChained(winningFork)) {
      Failure(
        new UnexpectedStateError(
          s"Inserting fork ${winningFork.map(_.header.id).mkString(",")} at height ${winningFork.map(_.header.height).mkString(",")} illegal"
        )
      )
    } else
      winningFork
        .foldLeft(Try((ListBuffer.empty[ApiFullBlock], ListBuffer.empty[Block], ListBuffer.empty[BufferedBlockInfo]))) {
          case (f @ Failure(_), _) =>
            f
          case (Success((newApiBlocksAcc, newBlocksAcc, toRemoveAcc)), apiBlock) =>
            BlockBuilder(
              apiBlock,
              Some(
                newBlocksAcc.lastOption
                  .collect { case b if b.header.id == apiBlock.header.parentId => BufferedBlockInfo.fromBlock(b) }
                  .getOrElse(blockBuffer.byId(apiBlock.header.parentId))
              )
            ).map { newBlock =>
              val newApiBlocks = newApiBlocksAcc :+ apiBlock
              val newBlocks    = newBlocksAcc :+ newBlock
              val toRemove =
                toRemoveAcc ++ blockBuffer.byHeight
                  .get(apiBlock.header.height)
                  .filter(_.headerId != apiBlock.header.id)
              (newApiBlocks, newBlocks, toRemove)
            }
        }
        .map { case (newApiBlocks, newBlocks, supersededBlocks) =>
          val newBlockBuffer = blockBuffer.addFork(newBlocks, supersededBlocks)
          val newInputsByHeight =
            inputsByHeightBuffer.removedAll(supersededBlocks.map(_.height)) ++
            newApiBlocks
              .map(b =>
                b.header.height ->
                b.transactions.transactions
                  .flatMap(tx => tx.outputs.map(o => (o.boxId, (o.address, o.value))).toMap)
                  .toMap
              )
              .toMap

          val newBoxesByHeightBuffer =
            newApiBlocks
              .map(b =>
                b.header.height -> b.transactions.transactions
                  .map { tx =>
                    val inputs = tx.inputs.map { i =>
                      def oAddr         = utxoState.addressByUtxo(i.boxId)
                      def oVal          = utxoState.utxosByAddress(oAddr)(i.boxId)
                      val (iAddr, iVal) = newInputsByHeight(b.header.height).getOrElse(i.boxId, (oAddr, oVal))
                      (i.boxId, iAddr, iVal)
                    }
                    tx.id -> (inputs, tx.outputs.map(o => (o.boxId, o.address, o.value)))
                  }
              )
              .toMap

          ForkInserted(newBlocks.toList, supersededBlocks.toList) -> copy(
            blockBuffer = newBlockBuffer,
            newInputsByHeight,
            boxesByHeightBuffer.removedAll(supersededBlocks.map(_.height)) ++ newBoxesByHeightBuffer
          )
        }

  def getLastCachedBlock: Option[BufferedBlockInfo] = blockBuffer.byHeight.lastOption.map(_._2)

  def persistedEpochIndexes: SortedSet[Int] = lastBlockIdInEpoch.keySet

  /** Genesis block is not part of a cache as it has no parent so
    * we assert that any block either has its parent cached or its a first block
    */
  def hasParent(block: ApiFullBlock): Boolean =
    blockBuffer.byId.contains(block.header.parentId) || block.header.height == 1

  def hasParentAndIsChained(fork: List[ApiFullBlock]): Boolean =
    fork.size > 1 &&
    blockBuffer.byId.contains(fork.head.header.parentId) &&
    fork.sliding(2).forall {
      case first :: second :: Nil =>
        first.header.id == second.header.parentId
      case _ =>
        false
    }

  def findMissingEpochIndexes: TreeSet[Int] =
    if (lastBlockIdInEpoch.isEmpty || lastBlockIdInEpoch.size == 1)
      TreeSet.empty
    else
      TreeSet((lastBlockIdInEpoch.head._1 to lastBlockIdInEpoch.last._1): _*)
        .diff(lastBlockIdInEpoch.keySet)

  override def toString: String = {
    val existingEpochs = persistedEpochIndexes
    val cachedHeights  = blockBuffer.heights

    def headStr(xs: SortedSet[Int]) = xs.headOption.map(h => s"[$h").getOrElse("")

    def lastStr(xs: SortedSet[Int]) =
      if (xs.isEmpty) ""
      else xs.lastOption.filterNot(xs.headOption.contains).map(h => s" - $h]").getOrElse("]")

    s"utxo count: ${utxoState.addressByUtxo.size}, non-empty-address count: ${utxoState.utxosByAddress.size}, " +
    s"persisted Epochs: ${existingEpochs.size}${headStr(existingEpochs)}${lastStr(existingEpochs)}, " +
    s"blocks cache size (heights): ${cachedHeights.size}${headStr(cachedHeights)}${lastStr(cachedHeights)}"
  }

}

object ChainState {

  case class BufferedBlockInfo(headerId: BlockId, parentId: BlockId, timestamp: Long, height: Int, info: BlockInfo)

  def empty: ChainState = apply(TreeMap.empty, UtxoState.empty)

  def apply(bufferedInfoByEpochIndex: TreeMap[Int, BufferedBlockInfo], utxoState: UtxoState): ChainState =
    ChainState(
      bufferedInfoByEpochIndex.map { case (epochIndex, blockInfo) => epochIndex -> blockInfo.headerId },
      BlockBuffer(
        bufferedInfoByEpochIndex.toSeq.map(i => i._2.headerId -> i._2).toMap,
        bufferedInfoByEpochIndex.map(i => i._2.height -> i._2)
      ),
      Map.empty,
      TreeMap.empty,
      utxoState
    )

  object BufferedBlockInfo {

    def fromBlock(b: Block): BufferedBlockInfo =
      BufferedBlockInfo(b.header.id, b.header.parentId, b.header.timestamp, b.header.height, b.info)
  }

  case class BlockBuffer(byId: Map[BlockId, BufferedBlockInfo], byHeight: SortedMap[Int, BufferedBlockInfo]) {
    def isEmpty: Boolean = byId.isEmpty || byHeight.isEmpty

    def heights: SortedSet[Int] = byHeight.keySet

    def addBlock(block: Block): BlockBuffer =
      BlockBuffer(
        byId.updated(block.header.id, BufferedBlockInfo.fromBlock(block)),
        byHeight.updated(block.header.height, BufferedBlockInfo.fromBlock(block))
      )

    def addFork(newFork: ListBuffer[Block], supersededFork: ListBuffer[BufferedBlockInfo]): BlockBuffer =
      BlockBuffer(
        (byId -- supersededFork.map(_.headerId)) ++ newFork
          .map(b => b.header.id -> BufferedBlockInfo.fromBlock(b)),
        byHeight ++ newFork.map(b => b.header.height -> BufferedBlockInfo.fromBlock(b))
      )

    def blockRelationsByHeight(heightRange: Seq[Int]): Seq[(Int, BlockRel)] =
      heightRange
        .map { height =>
          val info = byHeight(height)
          height -> BlockRel(info.headerId, info.parentId)
        }

    def flushEpoch(heightRange: Seq[Int]): BlockBuffer =
      BlockBuffer(
        byId -- heightRange.flatMap(byHeight.get).map(_.headerId),
        byHeight -- heightRange
      )
  }

}
