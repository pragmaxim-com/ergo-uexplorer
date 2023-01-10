package org.ergoplatform.uexplorer.plugin.alert

import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.{Address, BoxId, Const}
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateLike, UtxoStateWithPool, UtxoStateWithoutPool}
import org.ergoplatform.uexplorer.plugin.alert.Detector.AlertMessage
import HighValueDetector.*
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource

import java.text.DecimalFormat
import scala.collection.immutable.ArraySeq

class HighValueDetector(txErgValueThreshold: Long, blockErgValueThreshold: Long) extends Detector {

  def inspectNewPoolTx(
    tx: ApiTransaction,
    utxoStateWoPool: UtxoStateWithoutPool,
    utxoStateWithPool: UtxoStateWithPool,
    graphTraversalSource: GraphTraversalSource
  ): List[AlertMessage] = {
    val outputsWithoutPaybacksAndFees =
      tx.outputs.filterNot(o => tx.inputs.exists(_.boxId == o.boxId) || o.address == Const.FeeContract.address)
    Option(outputsWithoutPaybacksAndFees.iterator.map(_.value).sum)
      .filter(_ >= txErgValueThreshold * Const.NanoOrder)
      .map { value =>
        val inputAddresses = tx.inputs.iterator.map(_.boxId).flatMap(utxoStateWoPool.addressByUtxo.get).toSet
        val inputAddressesSum =
          inputAddresses.flatMap(utxoStateWoPool.utxosByAddress.get).foldLeft(0L) { case (acc, valueByBox) =>
            acc + valueByBox.values.sum
          }
        val outputAddresses = outputsWithoutPaybacksAndFees.iterator.map(_.address).toSet
        val outputAddressesSum =
          outputAddresses.flatMap(utxoStateWoPool.utxosByAddress.get).foldLeft(0L) { case (acc, valueByBox) =>
            acc + valueByBox.values.sum
          }
        val fmtValue              = valueFormat.format(value / Const.NanoOrder)
        val fmtInputAddressesSum  = valueFormat.format(inputAddressesSum / Const.NanoOrder)
        val fmtOutputAddressesSum = valueFormat.format(outputAddressesSum / Const.NanoOrder)
        s"${inputAddresses.size} addresses with total of $fmtInputAddressesSum Erg ===> $fmtValue Erg ===> ${outputAddresses.size} addresses with total of $fmtOutputAddressesSum Erg"
      }
      .map(msg => s"https://explorer.ergoplatform.com/en/transactions/${tx.id} $msg")
      .toList
  }

  def inspectNewBlock(
    newBlock: Block,
    utxoStateWoPool: UtxoStateWithoutPool,
    graphTraversalSource: GraphTraversalSource
  ): List[AlertMessage] = List.empty

}

object HighValueDetector {

  val valueFormat = new DecimalFormat("#,###")
}
