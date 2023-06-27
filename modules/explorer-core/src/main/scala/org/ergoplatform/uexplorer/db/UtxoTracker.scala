package org.ergoplatform.uexplorer.db

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.Const.Protocol.{Emission, Foundation}
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.MutableMapPimp
import org.ergoplatform.uexplorer.parser.ErgoTreeParser
import org.ergoplatform.uexplorer.Storage
import scala.collection.mutable
import scala.collection.compat.immutable.ArraySeq
import scala.util.Try
import java.util
import scala.collection.immutable.VectorBuilder
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*
import UtxoTracker.*

case class InputRecords(
  byErgoTree: mutable.Map[ErgoTreeHex, mutable.Set[BoxId]],
  byErgoTreeT8: mutable.Map[ErgoTreeT8Hex, mutable.Set[BoxId]],
  byTxId: mutable.Map[TxId, mutable.Map[ErgoTreeHex, mutable.Set[BoxId]]]
)

object UtxoTracker {

  def getBlockWithInputs(
    b: LinkedBlock,
    storage: Storage
  )(implicit enc: ErgoAddressEncoder): Try[BlockWithInputs] = Try {

    val outputLookup =
      b.outputRecords.iterator
        .map(o => (o.boxId, (o.ergoTreeHex, o.ergoTreeT8Hex)))
        .toMap

    val byErgoTree   = mutable.Map.empty[ErgoTreeHex, mutable.Set[BoxId]]
    val byErgoTreeT8 = mutable.Map.empty[ErgoTreeT8Hex, mutable.Set[BoxId]]
    val byTxId       = mutable.Map.empty[TxId, mutable.Map[ErgoTreeHex, mutable.Set[BoxId]]] // TODO populate

    b.b.transactions.transactions
      .foreach {
        case tx if tx.id == Emission.tx =>
          adjustMultiSet(byErgoTree, Emission.ergoTree, Emission.inputBox)
          adjustMultiSet(byErgoTreeT8, Emission.ergoTreeT8Hex, Emission.inputBox)
        case tx if tx.id == Foundation.tx =>
          adjustMultiSet(byErgoTree, Foundation.ergoTree, Foundation.inputBox)
          adjustMultiSet(byErgoTreeT8, Foundation.ergoTreeT8Hex, Foundation.inputBox)
        case tx =>
          val (cached, notCached) = tx.inputs.iterator.map(_.boxId).partition(outputLookup.contains)

          cached.foreach { boxId =>
            val (et, etT8Opt) = outputLookup(boxId)
            adjustMultiSet(byErgoTree, et, boxId)
            etT8Opt.foreach { t8 =>
              adjustMultiSet(byErgoTreeT8, t8, boxId)

            }
          }

          notCached
            .map { inputBoxId =>
              storage
                .getErgoTreeHexByUtxo(inputBoxId)
                .getOrElse(
                  throw new IllegalStateException(
                    s"Input boxId $inputBoxId of block ${b.b.header.id} at height ${b.info.height} not found in utxo state"
                  )
                ) -> inputBoxId
            }
            .toSeq
            .groupBy(_._1)
            .foreach { case (et, boxes) =>
              boxes
                .foreach(b => adjustMultiSet(byErgoTree, et, b._2))

              ErgoTreeParser
                .ergoTreeHex2T8Hex(et)
                .getOrElse(
                  throw new IllegalStateException(
                    s"Template of ergoTree $et of block ${b.b.header.id} at height ${b.info.height} cannot be extracted"
                  )
                )
                .foreach { t8 =>
                  boxes.foreach(b => adjustMultiSet(byErgoTreeT8, t8, b._2))
                }

            }
      }
    b.toBlockWithInputs(InputRecords(byErgoTree, byErgoTreeT8, byTxId))
  }

  private def adjustMultiSet[ET, B](m: mutable.Map[ET, mutable.Set[B]], et: ET, boxId: B) =
    m.adjust(et)(_.fold(mutable.Set(boxId))(_.addOne(boxId)))

}
