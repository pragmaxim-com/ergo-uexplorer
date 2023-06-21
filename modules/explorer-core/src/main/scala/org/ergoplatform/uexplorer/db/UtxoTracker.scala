package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.Const.Protocol.{Emission, Foundation}
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.*

import scala.collection.compat.immutable.ArraySeq
import scala.util.Try

object UtxoTracker {

  def apply(
    b: LinkedBlock,
    addressByUtxo: BoxId => Option[Address],
    utxoValueByAddress: (Address, BoxId) => Option[Value]
  ): Try[BlockWithInputs] = Try {
    val outputLookup =
      b.outputRecords.iterator
        .map(o => (o.boxId, (o.address, o.value)))
        .toMap

    def getInputAddressAndValue(inputBoxId: BoxId): (Address, Value) =
      outputLookup.getOrElse(
        inputBoxId, {
          val inputAddress = addressByUtxo(inputBoxId).getOrElse(
            throw new IllegalStateException(
              s"Input boxId $inputBoxId of block ${b.b.header.id} at height ${b.info.height} not found in utxo state"
            )
          )
          val value = utxoValueByAddress(inputAddress, inputBoxId).getOrElse(
            throw new IllegalStateException(
              s"Address $inputAddress of block ${b.b.header.id} at height ${b.info.height} not found in utxo state"
            )
          )
          inputAddress -> value
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
              Iterator(InputRecord(tx.id, Emission.inputBox, Emission.address, Emission.initialNanoErgs))
            case tx if tx.id == Foundation.tx =>
              Iterator(InputRecord(tx.id, Foundation.box, Foundation.address, Foundation.initialNanoErgs))
            case tx =>
              tx.inputs.iterator.map { i =>
                val (inputAddress, inputValue) = getInputAddressAndValue(i.boxId)
                InputRecord(tx.id, i.boxId, inputAddress, inputValue)
              }
          }
        }
    validateInputSumEqualsOutputSum(inputRecords, b.outputRecords)
    b.toBlockWithInputs(inputRecords)
  }

}
