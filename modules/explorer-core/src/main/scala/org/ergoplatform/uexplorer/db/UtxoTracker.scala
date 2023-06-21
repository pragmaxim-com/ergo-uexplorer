package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.Const.Protocol.{Emission, Foundation}
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.*

import scala.collection.compat.immutable.ArraySeq
import scala.util.Try
import scala.jdk.CollectionConverters.*

case class InputRecord(
  txId: TxId,
  boxId: BoxId,
  ergoTree: ErgoTreeHex,
  value: Value
)

object UtxoTracker {

  def apply(
    b: LinkedBlock,
    ergoTreeHexByUtxo: BoxId => Option[ErgoTreeHex],
    utxoValuesByErgoTreeHex: (ErgoTreeHex, IterableOnce[BoxId]) => Option[java.util.Map[BoxId, Value]]
  ): Try[BlockWithInputs] = Try {
    val outputLookup =
      b.outputRecords.iterator
        .map(o => (o.boxId, (o.ergoTree, o.value)))
        .toMap

    def validateInputSumEqualsOutputSum(
      inputs: IterableOnce[InputRecord],
      outputs: IterableOnce[OutputRecord]
    ): Unit = {
      val inputSum  = inputs.iterator.map(_.value).sum
      val outputSum = outputs.iterator.map(_.value).sum
      assert(
        inputSum == outputSum,
        s"Block ${b.b.header.id} invalid as sum of inputs $inputSum != $outputSum"
      )
    }

    val inputRecords =
      b.b.transactions.transactions
        .flatMap { tx =>
          tx match {
            case tx if tx.id == Emission.tx =>
              Iterator(InputRecord(tx.id, Emission.inputBox, Emission.ergoTree, Emission.initialNanoErgs))
            case tx if tx.id == Foundation.tx =>
              Iterator(InputRecord(tx.id, Foundation.box, Foundation.ergoTree, Foundation.initialNanoErgs))
            case tx =>
              val (cached, notCached) = tx.inputs.iterator.map(_.boxId).partition(outputLookup.contains)
              val cachedInputRecords =
                cached.map { boxId =>
                  val (et, inputValue) = outputLookup(boxId)
                  InputRecord(tx.id, boxId, et, inputValue)
                }

              val notCachedInputRecords =
                notCached
                  .map { inputBoxId =>
                    ergoTreeHexByUtxo(inputBoxId).getOrElse(
                      throw new IllegalStateException(
                        s"Input boxId $inputBoxId of block ${b.b.header.id} at height ${b.info.height} not found in utxo state"
                      )
                    ) -> inputBoxId
                  }
                  .toSeq
                  .groupBy(_._1)
                  .flatMap { case (et, values) =>
                    // converting java map to scala map :-/
                    utxoValuesByErgoTreeHex(et, values.iterator.map(_._2))
                      .getOrElse(
                        throw new IllegalStateException(
                          s"Address $et of block ${b.b.header.id} at height ${b.info.height} not found in utxo state"
                        )
                      )
                      .asScala
                      .map { case (boxId, value) =>
                        InputRecord(tx.id, boxId, et, value)
                      }
                  }
              notCachedInputRecords ++ cachedInputRecords
          }
        }
    validateInputSumEqualsOutputSum(inputRecords, b.outputRecords)
    b.toBlockWithInputs(inputRecords)
  }

}
