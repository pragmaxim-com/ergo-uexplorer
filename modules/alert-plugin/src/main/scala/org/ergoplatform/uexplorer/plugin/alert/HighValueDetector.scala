package org.ergoplatform.uexplorer.plugin.alert

import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.{Address, BoxId, Const}
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateLike, UtxoStateWithPool, UtxoStateWithoutPool}
import org.ergoplatform.uexplorer.plugin.alert.Detector.AlertMessage

import java.text.DecimalFormat
import scala.collection.immutable.ArraySeq

class HighValueDetector(txErgValueThreshold: Long, blockErgValueThreshold: Long) extends Detector {

  def inspectNewPoolTx(
    tx: ApiTransaction,
    utxoStateWoPool: UtxoStateWithoutPool,
    utxoStateWithPool: UtxoStateWithPool
  ): List[AlertMessage] = {
    val explorerAddress = s"https://explorer.ergoplatform.com/en/transactions/${tx.id}"
    HighValueDetector
      .detectHighValue(
        tx.inputs.iterator.map(_.boxId),
        tx.outputs.map(o => o.address -> o.value),
        utxoStateWithPool,
        txErgValueThreshold
      )
      .map(msg => s"$explorerAddress $msg")
      .toList
  }

  def inspectNewBlock(
    newBlock: Block,
    utxoStateWoPool: UtxoStateWithoutPool
  ): List[AlertMessage] = {
    val explorerAddress = s"https://explorer.ergoplatform.com/en/blocks/${newBlock.header.id}"
    HighValueDetector
      .detectHighValue(
        newBlock.inputs.iterator.map(_.boxId),
        newBlock.outputs.collect { case o if o.address != Const.genesisEmissionAddress => o.address -> o.value },
        utxoStateWoPool,
        blockErgValueThreshold
      )
      .map(msg => s"$explorerAddress $msg")
      .toList
  }
}

object HighValueDetector {

  private val valueFormat = new DecimalFormat("#,###")
  private val nanoOrder   = 1000000000d

  def detectHighValue(
    inputBoxIds: Iterator[BoxId],
    utxos: ArraySeq[(Address, Long)],
    utxoState: UtxoStateLike,
    ergValueThreshold: Long
  ): Option[AlertMessage] =
    Option(utxos.iterator.map(_._2).sum)
      .filter(_ >= ergValueThreshold * nanoOrder)
      .map { value =>
        val inputAddresses = inputBoxIds.flatMap(utxoState.addressByUtxo.get).toSet
        val inputAddressesSum =
          inputAddresses.flatMap(utxoState.utxosByAddress.get).foldLeft(0L) { case (acc, valueByBox) =>
            acc + valueByBox.values.sum
          }
        val outputAddresses = utxos.iterator.map(_._1).toSet
        val outputAddressesSum =
          outputAddresses.flatMap(utxoState.utxosByAddress.get).foldLeft(0L) { case (acc, valueByBox) =>
            acc + valueByBox.values.sum
          }
        val fmtValue              = valueFormat.format(value / nanoOrder)
        val fmtInputAddressesSum  = valueFormat.format(inputAddressesSum / nanoOrder)
        val fmtOutputAddressesSum = valueFormat.format(outputAddressesSum / nanoOrder)
        s"${inputAddresses.size} addresses with total of $fmtInputAddressesSum ===> $fmtValue ===> ${outputAddresses.size} addresses with total of $fmtOutputAddressesSum"
      }
}
