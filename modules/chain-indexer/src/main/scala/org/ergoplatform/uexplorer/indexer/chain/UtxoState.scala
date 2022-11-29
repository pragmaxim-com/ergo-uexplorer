package org.ergoplatform.uexplorer.indexer.chain

import akka.{Done, NotUsed}
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.pattern.StatusReply
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.{Address, BoxId}
import org.ergoplatform.uexplorer.indexer.chain.Epoch
import org.ergoplatform.uexplorer.indexer.*

import java.nio.file.{Path, Paths}
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.TreeMap
import scala.concurrent.Future
import scala.util.{Success, Try}

case class UtxoState(
  tempBoxesByHeightBuffer: TreeMap[Int, (ArraySeq[BoxId], ArraySeq[(BoxId, Address, Long)])],
  addressByUtxo: Map[BoxId, Address],
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

  def mergeEpochFromBuffer(heightRange: Seq[Int]): (ArraySeq[BoxId], UtxoState) = {
    val (inputIdsArr, utxosByAddressMap) =
      tempBoxesByHeightBuffer
        .range(heightRange.head, heightRange.last + 1)
        .foldLeft((ArraySeq.newBuilder[BoxId], Map.empty[Address, Map[BoxId, Long]])) {
          case ((inputBoxIdsAcc, utxosByAddressAcc), (_, (inputBoxIds, outputBoxIdsWithAddress))) =>
            (
              inputBoxIdsAcc ++= inputBoxIds,
              outputBoxIdsWithAddress
                .foldLeft(utxosByAddressAcc) { case (acc, (boxId, address, value)) =>
                  acc.adjust(address)(_.fold(Map(boxId -> value))(_.updated(boxId, value)))
                }
            )
        }
    val inputIds = inputIdsArr.result()
    inputIds -> mergeEpochFromBoxes(inputIds, utxosByAddressMap)
      .copy(tempBoxesByHeightBuffer = tempBoxesByHeightBuffer.removedAll(heightRange))
  }

  def mergeEpochFromBoxes(inputs: ArraySeq[BoxId], outputs: Map[Address, Map[BoxId, Long]]): UtxoState = {
    val boxIdsByAddressWithOutputs =
      outputs
        .foldLeft(utxosByAddress) { case (acc, (address, outputIds)) =>
          acc.putOrRemove(address) {
            case None                 => Some(outputIds)
            case Some(existingBoxIds) => Some(existingBoxIds ++ outputIds)
          }
        }
    val (inputsWithAddress, inputsWoAddress) =
      inputs.partition(i => addressByUtxo.contains(i) || outputs.exists(_._2.contains(i)))
    val inputIdsWithAddress =
      inputsWithAddress.map(boxId => boxId -> addressByUtxo.getOrElse(boxId, outputs.find(_._2.contains(boxId)).get._1))
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

    val newAddressByUtxo = outputs.iterator.flatMap { case (address, valueByUtxos) =>
      valueByUtxos.keySet.map(_ -> address)
    }
    UtxoState(
      tempBoxesByHeightBuffer,
      (addressByUtxo ++ newAddressByUtxo) -- inputs,
      boxIdsByAddressWoInputs,
      inputsWithoutAddress ++ inputsWoAddress
    )
  }
}

object UtxoState {

  def empty: UtxoState = UtxoState(TreeMap.empty, Map.empty, Map.empty, Set.empty)

}
