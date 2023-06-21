package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.Const.Protocol.{Emission, Foundation}
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.*

import scala.collection.compat.immutable.ArraySeq
import scala.util.Try

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
    utxoValueByErgoTreeHex: (ErgoTreeHex, BoxId) => Option[Value]
  ): Try[BlockWithInputs] = Try {
    val outputLookup =
      b.outputRecords.iterator
        .map(o => (o.boxId, (o.ergoTree, o.value)))
        .toMap

    def getInputErgoTreeAndValue(inputBoxId: BoxId): (ErgoTreeHex, Value) =
      outputLookup.getOrElse(
        inputBoxId, {
          val inputErgoTree = ergoTreeHexByUtxo(inputBoxId).getOrElse(
            throw new IllegalStateException(
              s"Input boxId $inputBoxId of block ${b.b.header.id} at height ${b.info.height} not found in utxo state"
            )
          )
          val value = utxoValueByErgoTreeHex(inputErgoTree, inputBoxId).getOrElse(
            throw new IllegalStateException(
              s"Address $inputErgoTree of block ${b.b.header.id} at height ${b.info.height} not found in utxo state"
            )
          )
          inputErgoTree -> value
        }
      )

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
              tx.inputs.iterator.map { i =>
                val (inputErgoTree, inputValue) = getInputErgoTreeAndValue(i.boxId)
                InputRecord(tx.id, i.boxId, inputErgoTree, inputValue)
              }
          }
        }
    validateInputSumEqualsOutputSum(inputRecords, b.outputRecords)
    b.toBlockWithInputs(inputRecords)
  }

}
