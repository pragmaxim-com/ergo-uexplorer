package org.ergoplatform.uexplorer.db

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.Const.Protocol.{Emission, Foundation}
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.db.UtxoTracker.*
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.parser.ErgoTreeParser

import java.util
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.VectorBuilder
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*
import scala.util.Try

case class InputRecords(
  byErgoTree: mutable.Map[ErgoTreeHex, mutable.Set[BoxId]],
  byErgoTreeT8: mutable.Map[ErgoTreeT8Hex, mutable.Set[BoxId]],
  byTxId: mutable.Map[TxId, mutable.Map[ErgoTreeHex, mutable.Set[BoxId]]]
)

object UtxoTracker {

  def getBlockWithInputs(
    b: LinkedBlock,
    storage: Storage
  )(implicit enc: ErgoAddressEncoder): Try[NormalizedBlock] = Try {

    val outputErgoTreeLookup =
      b.outputRecords.byErgoTree
        .flatMap(o => o._2.map(_.boxId -> o._1))
        .toMap
    val outputErgoTreeT8Lookup =
      b.outputRecords.byErgoTreeT8
        .flatMap(o => o._2.map(_.boxId -> o._1))
        .toMap

    val byErgoTree   = mutable.Map.empty[ErgoTreeHex, mutable.Set[BoxId]]
    val byErgoTreeT8 = mutable.Map.empty[ErgoTreeT8Hex, mutable.Set[BoxId]]
    val byTxId       = mutable.Map.empty[TxId, mutable.Map[ErgoTreeHex, mutable.Set[BoxId]]] // TODO populate

    b.b.transactions.transactions
      .foreach {
        case tx if tx.id == Emission.tx =>
          adjustMultiSet(byErgoTree, Emission.ergoTreeHex, Emission.inputBox)
          adjustMultiSet(byErgoTreeT8, Emission.ergoTreeT8Hex, Emission.inputBox)
        case tx if tx.id == Foundation.tx =>
          adjustMultiSet(byErgoTree, Foundation.ergoTreeHex, Foundation.inputBox)
          adjustMultiSet(byErgoTreeT8, Foundation.ergoTreeT8Hex, Foundation.inputBox)
        case tx =>
          val (cached, notCached) = tx.inputs.iterator.map(_.boxId).partition(outputErgoTreeLookup.contains)

          cached.foreach { boxId =>
            val ergoTree = outputErgoTreeLookup(boxId)
            adjustMultiSet(byErgoTree, ergoTree.hex, boxId)
            outputErgoTreeT8Lookup.get(boxId).foreach { ergoTreeT8 =>
              adjustMultiSet(byErgoTreeT8, ergoTreeT8.hex, boxId)
            }
          }

          notCached
            .map { inputBoxId =>
              storage
                .getErgoTreeHexByUtxo(inputBoxId)
                .getOrElse(
                  throw new IllegalStateException(
                    s"Input boxId $inputBoxId of block ${b.b.header.id} at height ${b.block.height} not found in utxo state"
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
                    s"Template of ergoTree $et of block ${b.b.header.id} at height ${b.block.height} cannot be extracted"
                  )
                )
                .foreach { t8 =>
                  boxes.foreach(b => adjustMultiSet(byErgoTreeT8, t8, b._2))
                }

            }
      }
    b.toNormalizedBlock(InputRecords(byErgoTree, byErgoTreeT8, byTxId))
  }

  private def adjustMultiSet[ET, B](m: mutable.Map[ET, mutable.Set[B]], et: ET, boxId: B) =
    m.adjust(et)(_.fold(mutable.Set(boxId))(_.addOne(boxId)))

}
