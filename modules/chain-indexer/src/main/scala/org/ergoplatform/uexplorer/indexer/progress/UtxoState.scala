package org.ergoplatform.uexplorer.indexer.progress

import akka.Done
import akka.actor.typed.ActorRef
import akka.pattern.StatusReply
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.{Address, BoxId}
import org.ergoplatform.uexplorer.indexer.progress.Epoch
import org.ergoplatform.uexplorer.indexer.*
import org.ergoplatform.uexplorer.indexer.progress.ProgressState.CachedBlock

import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.TreeMap
import scala.util.Try

case class UtxoState(
  boxesByHeight: TreeMap[Int, (List[BoxId], List[(BoxId, Address)])], // todo arraySeq
  byId: Map[BoxId, Address],
  byAddress: Map[Address, Set[BoxId]]
) {

  def addBestBlock(height: Int, inputs: List[BoxId], outputs: List[(BoxId, Address)]): UtxoState =
    copy(boxesByHeight = boxesByHeight.updated(height, inputs -> outputs))

  def addFork(
    newForkByHeight: Map[Int, (List[BoxId], List[(BoxId, Address)])],
    supersededForkHeights: List[Int]
  ): UtxoState = {
    val boxesByHeightWoSupersededFork = boxesByHeight.removedAll(supersededForkHeights)
    copy(boxesByHeight = boxesByHeightWoSupersededFork ++ newForkByHeight)
  }

  //TODO performance wise it could be faster to do one GroupBy over all 1024 blocks
  def persistEpoch(epochIndex: Int): (UtxoState, Set[BoxId]) = {
    val heightRange = Epoch.heightRangeForEpochIndex(epochIndex)
    def idsByAddress(boxIds: List[(BoxId, Address)]) =
      boxIds
        .groupBy(_._2)
        .view
        .mapValues(_.map(_._1).toSet)
        .toMap

    val (newById, newByAddress, inputsWoAddressSet) =
      heightRange.foldLeft((byId, byAddress, Set.empty[BoxId])) {
        case ((byIdAcc, byAddressAcc, inputsWoAddressAcc), height) =>
          val (inputs, outputs) = boxesByHeight(height)
          val boxIdsByAddressWithOutputs =
            idsByAddress(outputs).foldLeft(byAddressAcc) { case (acc, (address, outputIds)) =>
              acc.putOrRemove(address) {
                case None                 => Some(outputIds)
                case Some(existingBoxIds) => Some(existingBoxIds ++ outputIds)
              }
            }
          val addressesByOutputId = outputs.toMap
          val (inputsWithAddress, inputsWoAddress) =
            inputs.partition(i => byIdAcc.contains(i) || addressesByOutputId.contains(i))
          val inputIdsWithAddress =
            inputsWithAddress.map(boxId => boxId -> byIdAcc.getOrElse(boxId, addressesByOutputId(boxId)))
          val boxIdsByAddressWoInputs =
            idsByAddress(inputIdsWithAddress).foldLeft(boxIdsByAddressWithOutputs) { case (acc, (address, inputIds)) =>
              acc.putOrRemove(address) {
                case None                 => None
                case Some(existingBoxIds) => Option(existingBoxIds -- inputIds).filter(_.nonEmpty)
              }
            }

          ((byIdAcc ++ outputs) -- inputs, boxIdsByAddressWoInputs, inputsWoAddressAcc ++ inputsWoAddress)
      }
    UtxoState(boxesByHeight.removedAll(heightRange), newById, newByAddress) -> inputsWoAddressSet
  }
}
