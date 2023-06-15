package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.Const.Genesis.{Emission, Foundation}
import org.ergoplatform.uexplorer.{Address, BoxId, ProtocolSettings, Tx, Value}
import org.ergoplatform.uexplorer.node.ApiFullBlock

import scala.collection.compat.immutable.ArraySeq
import scala.util.Try

object LightBlockBuilder {

  def apply(
             apiBlock: ApiFullBlock,
             parentOpt: Option[VersionedBlock],
             addressByUtxo: BoxId => Option[Address],
             utxosByAddress: Address => Option[Map[BoxId, Value]]
  )(implicit
    ps: ProtocolSettings
  ): Try[LightBlock] =
    BlockInfoBuilder(apiBlock, parentOpt).map { blockInfo =>
      val apiHeader = apiBlock.header
      val outputLookup =
        apiBlock.transactions.transactions.iterator.flatMap(tx => tx.outputs.map(o => (o.boxId, (o.address, o.value)))).toMap
      val boxesByTx =
        apiBlock.transactions.transactions.zipWithIndex.map { case (tx, txIndex) =>
          val txOutputs = tx.outputs.map(o => (o.boxId, o.address, o.value))
          val inputs =
            tx match {
              case tx if tx.id == Emission.tx =>
                ArraySeq((Emission.box, Emission.address, Emission.initialNanoErgs))
              case tx if tx.id == Foundation.tx =>
                ArraySeq((Foundation.box, Foundation.address, Foundation.initialNanoErgs))
              case tx =>
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
            }
          Tx(tx.id, txIndex.toShort) -> (inputs, txOutputs)
        }
      LightBlock(apiHeader.id, apiHeader.parentId, apiHeader.timestamp, apiHeader.height, boxesByTx, blockInfo)
    }

}
