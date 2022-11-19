package org.ergoplatform.uexplorer.indexer.progress

import akka.{Done, NotUsed}
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.pattern.StatusReply
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.{Address, BoxId}
import org.ergoplatform.uexplorer.indexer.progress.Epoch
import org.ergoplatform.uexplorer.indexer.*

import java.nio.file.{Path, Paths}
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.TreeMap
import scala.concurrent.Future
import scala.util.{Success, Try}

case class UtxoState(
  tempBoxesByHeightBuffer: TreeMap[Int, (ArraySeq[BoxId], ArraySeq[(BoxId, Address, Long)])],
  addressById: Map[BoxId, Address],
  utxosByAddress: Map[Address, Map[BoxId, Long]],
  inputsWithoutAddress: Set[BoxId]
) {

  def bufferBestBlock(height: Int, inputs: ArraySeq[BoxId], outputs: ArraySeq[(BoxId, Address, Long)]): UtxoState =
    copy(tempBoxesByHeightBuffer = tempBoxesByHeightBuffer.updated(height, inputs -> outputs))

  def bufferFork(
    newForkByHeight: Map[Int, (ArraySeq[BoxId], ArraySeq[(BoxId, Address, Long)])],
    supersededForkHeights: List[Int]
  ): UtxoState = {
    val boxesByHeightWoSupersededFork = tempBoxesByHeightBuffer.removedAll(supersededForkHeights)
    copy(tempBoxesByHeightBuffer = boxesByHeightWoSupersededFork ++ newForkByHeight)
  }

  def mergeEpochFromBuffer(heightRange: Seq[Int]): ((ArraySeq[BoxId], ArraySeq[(BoxId, Address, Long)]), UtxoState) = {
    val (inputIdsArr, outputIdsWithAddressArr) =
      tempBoxesByHeightBuffer
        .range(heightRange.head, heightRange.last + 1)
        .foldLeft((ArraySeq.newBuilder[BoxId], ArraySeq.newBuilder[(BoxId, Address, Long)])) {
          case ((inputBoxIdsAcc, outputBoxIdsWithAddressAcc), (_, (inputBoxIds, outputBoxIdsWithAddress))) =>
            (inputBoxIdsAcc ++= inputBoxIds, outputBoxIdsWithAddressAcc ++= outputBoxIdsWithAddress)
        }
    val epochBoxes = inputIdsArr.result() -> outputIdsWithAddressArr.result()
    epochBoxes -> mergeEpochFromBoxes(epochBoxes._1, epochBoxes._2)
      .copy(tempBoxesByHeightBuffer = tempBoxesByHeightBuffer.removedAll(heightRange))
  }

  def mergeEpochFromBoxes(inputs: ArraySeq[BoxId], outputs: ArraySeq[(BoxId, Address, Long)]): UtxoState = {
    val boxIdsByAddressWithOutputs =
      outputs
        .groupBy(_._2)
        .view
        .mapValues(_.map(t => t._1 -> t._3).toMap)
        .foldLeft(utxosByAddress) { case (acc, (address, outputIds)) =>
          acc.putOrRemove(address) {
            case None                 => Some(outputIds)
            case Some(existingBoxIds) => Some(existingBoxIds ++ outputIds)
          }
        }
    val addressesByOutputId = outputs.map(t => t._1 -> t._2).toMap
    val (inputsWithAddress, inputsWoAddress) =
      inputs.partition(i => addressById.contains(i) || addressesByOutputId.contains(i))
    val inputIdsWithAddress =
      inputsWithAddress.map(boxId => boxId -> addressById.getOrElse(boxId, addressesByOutputId(boxId)))
    val boxIdsByAddressWoInputs =
      inputIdsWithAddress
        .groupBy(_._2)
        .view
        .mapValues(_.map(_._1).toSet)
        .foldLeft(boxIdsByAddressWithOutputs) { case (acc, (address, inputIds)) =>
          acc.putOrRemove(address) {
            case None                 => None
            case Some(existingBoxIds) => Option(existingBoxIds -- inputIds).filter(_.nonEmpty)
          }
        }

    UtxoState(
      tempBoxesByHeightBuffer,
      (addressById ++ addressesByOutputId) -- inputs,
      boxIdsByAddressWoInputs,
      inputsWithoutAddress ++ inputsWoAddress
    )
  }
}

object UtxoState {

  def empty: UtxoState = UtxoState(TreeMap.empty, Map.empty, Map.empty, Set.empty)

}
