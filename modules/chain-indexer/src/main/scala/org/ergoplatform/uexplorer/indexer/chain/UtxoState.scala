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
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Success, Try}

case class UtxoState(
  tempBoxesByHeightBuffer: TreeMap[Int, (ArraySeq[BoxId], ArraySeq[(BoxId, Address, Long)])],
  addressByUtxo: Map[BoxId, Address],
  utxosByAddress: Map[Address, mutable.Map[BoxId, Long]],
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

  def mergeEpochFromBuffer(heightRange: Seq[Int]): UtxoState = {
    val (inputsBuilder, outputs) =
      tempBoxesByHeightBuffer
        .range(heightRange.head, heightRange.last + 1)
        .foldLeft((ArraySeq.newBuilder[BoxId], Map.empty[Address, mutable.Map[BoxId, Long]])) {
          case ((inputBoxIdsAcc, utxosByAddressAcc), (_, (inputBoxIds, outputBoxIdsWithAddress))) =>
            (
              inputBoxIdsAcc ++= inputBoxIds,
              outputBoxIdsWithAddress
                .foldLeft(utxosByAddressAcc) { case (acc, (boxId, address, value)) =>
                  acc.adjust(address)(_.fold(mutable.Map(boxId -> value)) { x => x.put(boxId, value); x })
                }
            )
        }
    val inputs = inputsBuilder.result()
    val (newAddressByUtxo, boxIdsByAddressWithOutputs) =
      outputs
        .foldLeft(addressByUtxo -> utxosByAddress) { case ((addressByUtxoAcc, utxosByAddressAcc), (address, valueByUtxos)) =>
          (
            addressByUtxoAcc ++ valueByUtxos.keysIterator.map(_ -> address),
            utxosByAddressAcc.putOrRemove(address) {
              case None                 => Some(valueByUtxos)
              case Some(existingBoxIds) => Some(existingBoxIds ++ valueByUtxos)
            }
          )
        }
    val (inputsWithAddress, inputsWoAddress) =
      inputs.foldLeft(ArraySeq.newBuilder[(Address, BoxId)] -> ArraySeq.newBuilder[BoxId]) {
        case ((inputsWithAddressAcc, inputsWoAddressAcc), boxId) =>
          if (newAddressByUtxo.contains(boxId))
            inputsWithAddressAcc.addOne(newAddressByUtxo(boxId) -> boxId) -> inputsWoAddressAcc
          else
            inputsWithAddressAcc -> inputsWoAddressAcc.addOne(boxId)
      }
    val utxosByAddressWoInputs =
      inputsWithAddress
        .result()
        .groupBy(_._1)
        .view
        .mapValues(_.map(_._2))
        .foldLeft(boxIdsByAddressWithOutputs) { case (acc, (address, inputIds)) =>
          acc.putOrRemove(address) {
            case None                 => None
            case Some(existingBoxIds) => Option(existingBoxIds --= inputIds).filter(_.nonEmpty)
          }
        }
    UtxoState(
      tempBoxesByHeightBuffer.removedAll(heightRange),
      newAddressByUtxo -- inputs,
      utxosByAddressWoInputs,
      inputsWithoutAddress ++ inputsWoAddress.result()
    )
  }
}

object UtxoState {

  def empty: UtxoState = UtxoState(TreeMap.empty, Map.empty, Map.empty, Set.empty)

}
