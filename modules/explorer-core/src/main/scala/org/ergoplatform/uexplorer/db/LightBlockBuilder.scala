package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.Const.Genesis.{Emission, Foundation}
import org.ergoplatform.uexplorer.Storage.StorageVersion
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId, BoxesByTx, ProtocolSettings, Tx, Value}
import org.ergoplatform.uexplorer.node.ApiFullBlock

import scala.collection.compat.immutable.ArraySeq
import scala.util.Try

object LightBlockBuilder {

  def apply(
    apiBlock: ApiFullBlock,
    parentOpt: Option[BlockInfo],
    parentVersion: StorageVersion,
    addressByUtxo: BoxId => Option[Address],
    utxosByAddress: Address => Option[Map[BoxId, Value]]
  )(implicit
    ps: ProtocolSettings
  ): Try[LightBlock] =
    BlockInfoBuilder(apiBlock, parentOpt, parentVersion).map { blockInfo =>
      val apiHeader = apiBlock.header
      val outputLookup =
        apiBlock.transactions.transactions.iterator
          .flatMap(tx => tx.outputs.map(o => (o.boxId, (o.address, o.value))))
          .toMap
      val boxesByTx =
        apiBlock.transactions.transactions.zipWithIndex.map { case (tx, txIndex) =>
          val txOutputs = tx.outputs.map(o => (o.boxId, o.address, o.value))
          val txInputs =
            tx match {
              case tx if tx.id == Emission.tx =>
                ArraySeq((Emission.box, Emission.address, Emission.initialNanoErgs))
              case tx if tx.id == Foundation.tx =>
                ArraySeq((Foundation.box, Foundation.address, Foundation.initialNanoErgs))
              case tx =>
                val inputs =
                  tx.inputs.map { i =>
                    val valueByAddress = outputLookup.get(i.boxId)
                    val inputAddress =
                      valueByAddress
                        .map(_._1)
                        .orElse(addressByUtxo(i.boxId))
                        .getOrElse(
                          throw new IllegalStateException(
                            s"BoxId ${i.boxId} of block ${apiHeader.id} at height ${apiHeader.height} not found in utxo state" + txOutputs
                              .mkString("\n", "\n", "\n")
                          )
                        )
                    val inputValue =
                      valueByAddress
                        .map(_._2)
                        .orElse(utxosByAddress(inputAddress).flatMap(_.get(i.boxId)))
                        .getOrElse(
                          throw new IllegalStateException(
                            s"Address $inputAddress of block ${apiHeader.id} at height ${apiHeader.height} not found in utxo state" + txOutputs
                              .mkString("\n", "\n", "\n")
                          )
                        )
                    (i.boxId, inputAddress, inputValue)
                  }
                val inputSum  = inputs.iterator.map(_._3).sum
                val outputSum = txOutputs.iterator.map(_._3).sum
                assert(
                  inputSum == outputSum,
                  s"Transaction ${tx.id} at block ${apiBlock.header.id} invalid as sum of inputs $inputSum != $outputSum"
                )
                inputs
            }
          Tx(tx.id, txIndex.toShort) -> (txInputs, txOutputs)
        }
      LightBlock(apiHeader.id, boxesByTx, blockInfo)
    }

}

final case class LightBlock(headerId: BlockId, boxesByTx: BoxesByTx, info: BlockInfo)
