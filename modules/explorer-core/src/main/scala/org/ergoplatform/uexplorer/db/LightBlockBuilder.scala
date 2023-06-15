package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.Const.Genesis.{Emission, Foundation}
import org.ergoplatform.uexplorer.Storage.StorageVersion
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.*

import scala.collection.compat.immutable.ArraySeq
import scala.util.Try

object LightBlockBuilder {

  def apply(
    b: ApiFullBlock,
    bInfo: BlockInfo,
    addressByUtxo: BoxId => Option[Address],
    utxosByAddress: Address => Option[Map[BoxId, Value]]
  ): Try[LightBlock] = Try {
    val outputLookup =
      b.transactions.transactions.iterator
        .flatMap(tx => tx.outputs.map(o => (o.boxId, (o.address, o.value))))
        .toMap

    def getInputAddress(inputBoxId: BoxId) =
      outputLookup
        .get(inputBoxId)
        .map(_._1)
        .orElse(addressByUtxo(inputBoxId))
        .getOrElse(
          throw new IllegalStateException(
            s"BoxId $inputBoxId of block ${b.header.id} at height ${b.header.height} not found in utxo state"
          )
        )

    def getInputValue(inputAddress: Address, inputBoxId: BoxId) =
      outputLookup
        .get(inputBoxId)
        .map(_._2)
        .orElse(utxosByAddress(inputAddress).flatMap(_.get(inputBoxId)))
        .getOrElse(
          throw new IllegalStateException(
            s"Address $inputAddress of block ${b.header.id} at height ${b.header.height} not found in utxo state"
          )
        )

    def validateInputSumEqualsOutputSum(boxesByTx: BoxesByTx): Unit =
      boxesByTx.foreach { case (txId, (inputs, outputs)) =>
        val inputSum  = inputs.iterator.map(_._3).sum
        val outputSum = outputs.iterator.map(_._3).sum
        assert(
          inputSum == outputSum,
          s"Transaction $txId at block ${b.header.id} invalid as sum of inputs $inputSum != $outputSum"
        )
      }

    val boxesByTx =
      b.transactions.transactions
        .map { tx =>
          val txOutputs = tx.outputs.map(o => (o.boxId, o.address, o.value))
          val txInputs =
            tx match {
              case tx if tx.id == Emission.tx =>
                ArraySeq((Emission.box, Emission.address, Emission.initialNanoErgs))
              case tx if tx.id == Foundation.tx =>
                ArraySeq((Foundation.box, Foundation.address, Foundation.initialNanoErgs))
              case tx =>
                tx.inputs.map { i =>
                  val inputAddress = getInputAddress(i.boxId)
                  (i.boxId, inputAddress, getInputValue(inputAddress, i.boxId))
                }
            }
          tx.id -> (txInputs, txOutputs)
        }
    validateInputSumEqualsOutputSum(boxesByTx)
    LightBlock(b.header.id, boxesByTx, bInfo)
  }

}
