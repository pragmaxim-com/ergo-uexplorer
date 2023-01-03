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
    boxes: Iterator[(ArraySeq[(BoxId, Address, Long)], ArraySeq[(BoxId, Address, Long)])]
  ): UtxoState = {
    val (newAddressByUtxo, newUtxosByAddress) =
      boxes
        .foldLeft(addressByUtxo -> utxosByAddress) {
          case ((addressByUtxoAcc, utxosByAddressAcc), (inputBoxIds, outputBoxIdsWithAddress)) =>
            val newOutputBoxIdsByAddress =
              outputBoxIdsWithAddress
                .foldLeft(utxosByAddressAcc) { case (acc, (boxId, address, value)) =>
                  acc.adjust(address)(_.fold(Map(boxId -> value))(_.updated(boxId, value)))
                }
            val newOutputBoxIdsByAddressWoInputs =
              inputBoxIds
                .groupBy(_._2)
                .view
                .mapValues(_.map(_._1))
                .foldLeft(newOutputBoxIdsByAddress) { case (acc, (address, inputIds)) =>
                  acc.putOrRemove(address) {
                    case None                 => None
                    case Some(existingBoxIds) => Option(existingBoxIds.removedAll(inputIds)).filter(_.nonEmpty)
                  }
                }
            (
              addressByUtxoAcc ++ outputBoxIdsWithAddress.iterator.map(o => o._1 -> o._2) -- inputBoxIds.iterator.map(_._1),
              newOutputBoxIdsByAddressWoInputs
            )
        }
    UtxoState(
      newAddressByUtxo,
      newUtxosByAddress
    )
  }
}

object UtxoState extends LazyLogging {
  def empty: UtxoState = UtxoState(Map.empty, Map.empty)
}
