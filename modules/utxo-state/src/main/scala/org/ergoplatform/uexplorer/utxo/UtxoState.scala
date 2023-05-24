package org.ergoplatform.uexplorer.utxo

import com.google.common.collect.TreeMultiset
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.utxo.TopAddresses.*
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ApiTransaction}
import org.ergoplatform.uexplorer.*
import java.io.*
import java.nio.file.{Path, Paths}
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.{ArraySeq, TreeMap}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Success, Try}

case class UtxoState(
  addressByUtxo: Map[BoxId, Address],
  utxosByAddress: Map[Address, Map[BoxId, Value]],
  inputsByHeightBuffer: InputsByHeight,
  boxesByHeightBuffer: BoxesByHeight,
  topAddresses: TopAddresses
) {

  /** on-demand computation of UtxoState up to latest blocks which are not merged right away as we do not support rollback,
    * instead we merge only winner-fork blocks into UtxoState
    */
  def utxoStateWithCurrentEpochBoxes: UtxoState = mergeBufferedBoxes(Option.empty)._2

  def mergeBlockBoxes(
    height: Height,
    boxes: Iterator[(Tx, (Iterable[(BoxId, Address, Value)], Iterable[(BoxId, Address, Value)]))]
  ): UtxoState = {
    val (newAddressByUtxo, newUtxosByAddress, newTopAddresses) =
      boxes.foldLeft((addressByUtxo, utxosByAddress, topAddresses.nodeMap)) {
        case ((addressByUtxoAcc, utxosByAddressAcc, topAddressesAcc), (tx, (inputBoxes, outputBoxes))) =>
          val newOutputBoxIdsByAddress =
            outputBoxes
              .foldLeft(utxosByAddressAcc) { case (acc, (boxId, address, value)) =>
                acc.adjust(address)(_.fold(Map(boxId -> value))(_.updated(boxId, value)))
              }
          val newOutputBoxIdsByAddressWoInputs =
            inputBoxes
              .groupBy(_._2)
              .view
              .mapValues(_.map(_._1))
              .foldLeft(newOutputBoxIdsByAddress) { case (acc, (address, inputIds)) =>
                acc.putOrRemove(address) {
                  case None                 => None
                  case Some(existingBoxIds) => Option(existingBoxIds.removedAll(inputIds)).filter(_.nonEmpty)
                }
              }
          val boxesToMergeToAddresses =
            if (tx.id == Const.Genesis.Emission.tx || tx.id == Const.Genesis.Foundation.tx) {
              outputBoxes
            } else {
              inputBoxes ++ outputBoxes
            }

          val newTopAddressesAcc =
            boxesToMergeToAddresses
              .foldLeft(mutable.Map.empty[Address, Int]) { case (acc, (_, address, _)) =>
                acc.adjust(address)(_.fold(1)(_ + 1))
              }
              .foldLeft(topAddressesAcc) { case (acc, (address, boxCount)) =>
                acc.adjust(address) {
                  case None =>
                    Address.Stats(height, 1, boxCount)
                  case Some(Address.Stats(_, oldTxCount, oldBoxCount)) =>
                    Address.Stats(height, oldTxCount + 1, oldBoxCount + boxCount)
                }
              }
          (
            addressByUtxoAcc ++ outputBoxes.iterator.map(o => o._1 -> o._2) -- inputBoxes.iterator.map(_._1),
            newOutputBoxIdsByAddressWoInputs,
            newTopAddressesAcc
          )
      }

    copy(
      addressByUtxo  = newAddressByUtxo,
      utxosByAddress = newUtxosByAddress,
      topAddresses   = topAddresses.addOrUpdate(height, newTopAddresses)
    )

  }

  def mergeBufferedBoxes(
    heightRangeOpt: Option[Seq[Height]]
  ): (BoxesByHeight, UtxoState) = {
    val boxesByHeightSlice = heightRangeOpt
      .map { heightRange =>
        boxesByHeightBuffer.range(heightRange.head, heightRange.last + 1)
      }
      .getOrElse(boxesByHeightBuffer)

    val newUtxoState = mergeBlockBoxes(boxesByHeightSlice.lastKey, boxesByHeightSlice.iterator.flatMap(_._2.iterator))
    boxesByHeightSlice -> newUtxoState.copy(
      inputsByHeightBuffer = inputsByHeightBuffer -- boxesByHeightSlice.keysIterator,
      boxesByHeightBuffer  = boxesByHeightBuffer -- boxesByHeightSlice.keysIterator
    )
  }

  private def getInput(
    boxId: BoxId,
    blockId: BlockId,
    newInputsByHeight: Map[Height, Map[BoxId, (Address, Long)]]
  ) =
    addressByUtxo
      .get(boxId)
      .map(oAddr => (boxId, oAddr, utxosByAddress(oAddr)(boxId)))
      .getOrElse(
        newInputsByHeight.valuesIterator
          .collectFirst {
            case xs if xs.contains(boxId) =>
              val (a, v) = xs(boxId)
              (boxId, a, v)
          }
          .getOrElse(throw IllegalStateException(s"Box $boxId in block $blockId cannot be found"))
      )

  def bufferBestBlock(headerId: BlockId, height: Height, timestamp: Long, txs: ArraySeq[ApiTransaction]): UtxoState = {
    val newInputsByHeight = inputsByHeightBuffer.updated(
      height,
      txs
        .flatMap(tx => tx.outputs.map(o => (o.boxId, (o.address, o.value))).toMap)
        .toMap
    )
    val newBoxesByHeightBuffer = boxesByHeightBuffer.updated(
      height,
      txs.zipWithIndex.map { case (tx, txIndex) =>
        val inputs =
          tx match {
            case tx if tx.id == Const.Genesis.Emission.tx =>
              ArraySeq((Const.Genesis.Emission.box, Const.Genesis.Emission.address, Const.Genesis.Emission.initialNanoErgs))
            case tx if tx.id == Const.Genesis.Foundation.tx =>
              ArraySeq(
                (Const.Genesis.Foundation.box, Const.Genesis.Foundation.address, Const.Genesis.Foundation.initialNanoErgs)
              )
            case tx =>
              tx.inputs.map(i => getInput(i.boxId, headerId, newInputsByHeight))
          }
        Tx(tx.id, txIndex.toShort, height, timestamp) -> (inputs, tx.outputs.map(o => (o.boxId, o.address, o.value)))
      }
    )
    copy(
      inputsByHeightBuffer = newInputsByHeight,
      boxesByHeightBuffer  = newBoxesByHeightBuffer
    )
  }

  def bufferFork(newApiBlocks: ListBuffer[ApiFullBlock], supersededBlocks: ListBuffer[BlockMetadata]): UtxoState = {
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
          b.header.height -> b.transactions.transactions.zipWithIndex
            .map { case (tx, txIndex) =>
              val inputs = tx.inputs.map(i => getInput(i.boxId, b.header.id, newInputsByHeight))
              Tx(tx.id, txIndex.toShort, b.header.height, b.header.timestamp) -> (inputs, tx.outputs
                .map(o => (o.boxId, o.address, o.value)))
            }
        )
        .toMap
    copy(
      inputsByHeightBuffer = newInputsByHeight,
      boxesByHeightBuffer  = boxesByHeightBuffer.removedAll(supersededBlocks.map(_.height)) ++ newBoxesByHeightBuffer
    )
  }
}

object UtxoState extends LazyLogging {
  def empty: UtxoState = UtxoState(Map.empty, Map.empty, Map.empty, TreeMap.empty, TopAddresses.empty)
}