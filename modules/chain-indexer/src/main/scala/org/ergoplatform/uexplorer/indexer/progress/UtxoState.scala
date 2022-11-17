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
  boxesByHeight: TreeMap[Int, (ArraySeq[BoxId], ArraySeq[(BoxId, Address)])],
  addressById: Map[BoxId, Address],
  utxosByAddress: Map[Address, Set[BoxId]],
  inputsWithoutAddress: Set[BoxId]
) {

  def addBestBlock(height: Int, inputs: ArraySeq[BoxId], outputs: ArraySeq[(BoxId, Address)]): UtxoState =
    copy(boxesByHeight = boxesByHeight.updated(height, inputs -> outputs))

  def addFork(
    newForkByHeight: Map[Int, (ArraySeq[BoxId], ArraySeq[(BoxId, Address)])],
    supersededForkHeights: List[Int]
  ): UtxoState = {
    val boxesByHeightWoSupersededFork = boxesByHeight.removedAll(supersededForkHeights)
    copy(boxesByHeight = boxesByHeightWoSupersededFork ++ newForkByHeight)
  }

  def finishEpoch(epochIndex: Int): ((ArraySeq[BoxId], ArraySeq[(BoxId, Address)]), UtxoState) = {
    val heightRange = Epoch.heightRangeForEpochIndex(epochIndex)
    val (inputIdsArr, outputIdsWithAddressArr) =
      boxesByHeight
        .range(heightRange.head, heightRange.last + 1)
        .foldLeft((ArraySeq.newBuilder[BoxId], ArraySeq.newBuilder[(BoxId, Address)])) {
          case ((inputBoxIdsAcc, outputBoxIdsWithAddressAcc), (_, (inputBoxIds, outputBoxIdsWithAddress))) =>
            (inputBoxIdsAcc ++= inputBoxIds, outputBoxIdsWithAddressAcc ++= outputBoxIdsWithAddress)
        }
    val epochBoxes = inputIdsArr.result() -> outputIdsWithAddressArr.result()
    epochBoxes -> UtxoState
      .mergeEpoch(this, epochBoxes._1, epochBoxes._2)
      .copy(boxesByHeight = boxesByHeight.removedAll(heightRange))
  }
}

object UtxoState extends LazyLogging {

  def empty: UtxoState = UtxoState(TreeMap.empty, Map.empty, Map.empty, Set.empty)

  private def idsByAddress(boxIds: ArraySeq[(BoxId, Address)]) =
    boxIds
      .groupBy(_._2)
      .view
      .mapValues(_.map(_._1).toSet)

  def mergeEpoch(s: UtxoState, inputs: ArraySeq[BoxId], outputs: ArraySeq[(BoxId, Address)]): UtxoState = {
    val boxIdsByAddressWithOutputs =
      idsByAddress(outputs).foldLeft(s.utxosByAddress) { case (acc, (address, outputIds)) =>
        acc.putOrRemove(address) {
          case None                 => Some(outputIds)
          case Some(existingBoxIds) => Some(existingBoxIds ++ outputIds)
        }
      }
    val addressesByOutputId = outputs.toMap
    val (inputsWithAddress, inputsWoAddress) =
      inputs.partition(i => s.addressById.contains(i) || addressesByOutputId.contains(i))
    val inputIdsWithAddress =
      inputsWithAddress.map(boxId => boxId -> s.addressById.getOrElse(boxId, addressesByOutputId(boxId)))
    val boxIdsByAddressWoInputs =
      idsByAddress(inputIdsWithAddress).foldLeft(boxIdsByAddressWithOutputs) { case (acc, (address, inputIds)) =>
        acc.putOrRemove(address) {
          case None                 => None
          case Some(existingBoxIds) => Option(existingBoxIds -- inputIds).filter(_.nonEmpty)
        }
      }

    UtxoState(
      s.boxesByHeight,
      (s.addressById ++ outputs) -- inputs,
      boxIdsByAddressWoInputs,
      s.inputsWithoutAddress ++ inputsWoAddress
    )
  }

}
