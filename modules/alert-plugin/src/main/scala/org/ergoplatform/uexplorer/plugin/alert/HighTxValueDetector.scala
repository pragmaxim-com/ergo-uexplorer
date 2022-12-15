package org.ergoplatform.uexplorer.plugin.alert

import org.ergoplatform.uexplorer.{Address, BoxId}
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateWithPool, UtxoStateWithoutPool}
import org.ergoplatform.uexplorer.plugin.alert.Detector.AlertMessage

import java.text.DecimalFormat

class HighTxValueDetector(valueThreshold: Long) extends Detector {

  private val valueFormat = new DecimalFormat("#,###")
  private val nanoOrder   = 1000000000d

  def inspect(
    newTx: ApiTransaction,
    utxoStateWoPool: UtxoStateWithoutPool,
    utxoStateWithPool: UtxoStateWithPool
  ): List[AlertMessage] =
    Option(newTx.outputs.map(_.value).sum)
      .filter(_ >= valueThreshold)
      .map { value =>
        val inputAddresses = newTx.inputs.map(_.boxId).flatMap(utxoStateWithPool.addressByUtxo.get).toSet
        val inputAddressesSum =
          inputAddresses.flatMap(utxoStateWithPool.utxosByAddress.get).foldLeft(0L) { case (acc, valueByBox) =>
            acc + valueByBox.values.sum
          }
        val outputAddresses = newTx.outputs.map(_.address).toSet
        val outputAddressesSum =
          outputAddresses.flatMap(utxoStateWithPool.utxosByAddress.get).foldLeft(0L) { case (acc, valueByBox) =>
            acc + valueByBox.values.sum
          }
        val explorerAddress       = s"https://explorer.ergoplatform.com/en/transactions/${newTx.id}"
        val fmtValue              = valueFormat.format(value / nanoOrder)
        val fmtInputAddressesSum  = valueFormat.format(inputAddressesSum / nanoOrder)
        val fmtOutputAddressesSum = valueFormat.format(outputAddressesSum / nanoOrder)
        s"${inputAddresses.size} addresses with total of $fmtInputAddressesSum ===> $fmtValue ===> ${outputAddresses.size} addresses with total of $fmtOutputAddressesSum\n$explorerAddress"
      }
      .toList

}
