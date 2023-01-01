package org.ergoplatform.uexplorer.indexer.utxo

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.pattern.StatusReply
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.*
import org.ergoplatform.uexplorer.indexer.chain.Epoch
import org.ergoplatform.uexplorer.{Address, BoxId, Const, TxId}

import java.io.*
import java.nio.file.{Path, Paths}
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.{ArraySeq, TreeMap}
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Success, Try}

case class UtxoState(
  addressByUtxo: Map[BoxId, Address],
  utxosByAddress: Map[Address, Map[BoxId, Long]]
) {

  def mergeBoxes(
    boxes: Iterator[(ArraySeq[BoxId], ArraySeq[(BoxId, Address, Long)])]
  ): Try[UtxoState] = {
    val (inputsBuilder, newAddressByUtxo, newUtxosByAddress) =
      boxes
        .foldLeft((ArraySeq.newBuilder[BoxId], addressByUtxo, utxosByAddress)) {
          case ((inputBoxIdsAcc, addressByUtxoAcc, utxosByAddressAcc), (inputBoxIds, outputBoxIdsWithAddress)) =>
            (
              inputBoxIdsAcc.addAll(inputBoxIds),
              addressByUtxoAcc ++ outputBoxIdsWithAddress.iterator.map(o => o._1 -> o._2),
              outputBoxIdsWithAddress
                .foldLeft(utxosByAddressAcc) { case (acc, (boxId, address, value)) =>
                  acc.adjust(address)(_.fold(Map(boxId -> value))(_.updated(boxId, value)))
                }
            )
        }

    val inputs = inputsBuilder.result()
    Try {
      inputs.foldLeft(mutable.Map.empty[Address, mutable.Set[BoxId]]) { case (inputsWithAddressAcc, boxId) =>
        if (newAddressByUtxo.contains(boxId)) {
          val address = newAddressByUtxo(boxId)
          inputsWithAddressAcc.adjust(address)(
            _.fold(mutable.Set(boxId))(_.addOne(boxId))
          )
        } else if (Const.genesisBoxes.contains(boxId)) {
          inputsWithAddressAcc
        } else {
          throw new IllegalStateException(s"Input boxId $boxId is missing corresponding utxo in UtxoState")
        }
      }
    }.map { inputsWithAddress =>
      val utxosByAddressWoInputs =
        inputsWithAddress
          .foldLeft(newUtxosByAddress) { case (acc, (address, inputIds)) =>
            acc.putOrRemove(address) {
              case None                 => None
              case Some(existingBoxIds) => Option(existingBoxIds.removedAll(inputIds)).filter(_.nonEmpty)
            }
          }
      UtxoState(
        newAddressByUtxo -- inputs,
        utxosByAddressWoInputs
      )
    }
  }
}

object UtxoState extends LazyLogging {
  def empty: UtxoState = UtxoState(Map.empty, Map.empty)
}
