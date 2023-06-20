package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.Const.Protocol.{Emission, Foundation}
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.db.Record

import scala.collection.compat.immutable.ArraySeq
import scala.util.Try

object LightBlockBuilder {

  def apply(
    b: ApiFullBlock,
    bInfo: BlockInfo,
    addressByUtxo: BoxId => Option[Address],
    utxoValueByAddress: (Address, BoxId) => Option[Value]
  ): Try[LightBlock] = Try {
    val outputLookup =
      b.transactions.transactions.iterator
        .flatMap(tx => tx.outputs.map(o => (o.boxId, (o.address, o.value))))
        .toMap

    def getInputAddressAndValue(inputBoxId: BoxId): (Address, Value) =
      outputLookup.getOrElse(
        inputBoxId, {
          val inputAddress = addressByUtxo(inputBoxId).getOrElse(
            throw new IllegalStateException(
              s"Input boxId $inputBoxId of block ${b.header.id} at height ${b.header.height} not found in utxo state"
            )
          )
          val value = utxoValueByAddress(inputAddress, inputBoxId).getOrElse(
            throw new IllegalStateException(
              s"Address $inputAddress of block ${b.header.id} at height ${b.header.height} not found in utxo state"
            )
          )
          inputAddress -> value
        }
      )

    def validateInputSumEqualsOutputSum(
      inputs: IterableOnce[Record],
      outputs: IterableOnce[Record]
    ): Unit = {
      val inputSum  = inputs.iterator.map(_.value).sum
      val outputSum = outputs.iterator.map(_.value).sum
      assert(
        inputSum == outputSum,
        s"Block ${b.header.id} invalid as sum of inputs $inputSum != $outputSum"
      )
    }

    val outputs =
      b.transactions.transactions
        .flatMap(tx => tx.outputs.map(o => Record(tx.id, o.boxId, o.address, o.value)))

    val inputs =
      b.transactions.transactions
        .flatMap { tx =>
          tx match {
            case tx if tx.id == Emission.tx =>
              Iterator(Record(tx.id, Emission.inputBox, Emission.address, Emission.initialNanoErgs))
            case tx if tx.id == Foundation.tx =>
              Iterator(Record(tx.id, Foundation.box, Foundation.address, Foundation.initialNanoErgs))
            case tx =>
              tx.inputs.iterator.map { i =>
                val (inputAddress, inputValue) = getInputAddressAndValue(i.boxId)
                Record(tx.id, i.boxId, inputAddress, inputValue)
              }
          }
        }
    validateInputSumEqualsOutputSum(inputs, outputs)
    LightBlock(b.header.id, inputs, outputs, bInfo)
  }

}
