package org.ergoplatform.uexplorer.plugin.alert

import org.ergoplatform.uexplorer.{Address, BoxId}
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.alert.Detector.AlertMessage

import java.text.DecimalFormat

class HighTxValueDetector(valueThreshold: Long) extends Detector {

  private val valueFormat = new DecimalFormat("#,###")
  private val nanoOrder   = 1000000000d

  def inspect(
    tx: ApiTransaction,
    addressByUtxo: Map[BoxId, Address],
    utxosByAddress: Map[Address, Map[BoxId, Long]]
  ): List[AlertMessage] =
    Option(tx.outputs.map(_.value).sum)
      .filter(_ >= valueThreshold)
      .map { value =>
        val inputAddresses = tx.inputs.map(_.boxId).flatMap(addressByUtxo.get).toSet
        val inputAddressesSum =
          inputAddresses.flatMap(utxosByAddress.get).foldLeft(0L) { case (acc, valueByBox) => acc + valueByBox.values.sum }
        val outputAddresses = tx.outputs.map(_.address).toSet
        val outputAddressesSum =
          outputAddresses.flatMap(utxosByAddress.get).foldLeft(0L) { case (acc, valueByBox) => acc + valueByBox.values.sum }
        val explorerAddress       = s"https://explorer.ergoplatform.com/en/transactions/${tx.id}"
        val fmtValue              = valueFormat.format(value / nanoOrder)
        val fmtInputAddressesSum  = valueFormat.format(inputAddressesSum / nanoOrder)
        val fmtOutputAddressesSum = valueFormat.format(outputAddressesSum / nanoOrder)
        s"${inputAddresses.size} addresses with total of $fmtInputAddressesSum ===> $fmtValue ===> ${outputAddresses.size} addresses with total of $fmtOutputAddressesSum\n$explorerAddress"
      }
      .toList

}
