package org.ergoplatform.uexplorer.plugin.alert

import org.ergoplatform.uexplorer.db.{BestBlockInserted, Block}
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.alert.Detector.AlertMessage
import HighValueDetector.*
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.utxo.UtxoState
import org.slf4j.{Logger, LoggerFactory}

import java.text.DecimalFormat
import scala.collection.immutable.ArraySeq

class HighValueDetector(txErgValueThreshold: Long, blockErgValueThreshold: Long) extends Detector {

  private val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def inspectNewPoolTx(
    tx: ApiTransaction,
    utxoState: UtxoState,
    graphTraversalSource: GraphTraversalSource
  ): List[TxMatch] = {
    def sumAddressValues(addresses: Set[Address]): Map[Address, Address.State] =
      addresses
        .flatMap(a => utxoState.getUtxosByAddress(a).map(a -> _))
        .foldLeft(Map.empty[Address, Address.State]) { case (acc, (address, valueByBox)) =>
          acc.updated(address, Address.State(valueByBox.values.sum, utxoState.getAddressStats(address)))
        }

    val outputsWithoutPaybacksAndFees =
      tx.outputs.filterNot(o =>
        tx.inputs.exists(i =>
          utxoState.getAddressByUtxo(i.boxId).contains(o.address)
        ) || o.address == Const.FeeContract.address
      )
    Option(outputsWithoutPaybacksAndFees.iterator.map(_.value).sum)
      .filter(_ >= txErgValueThreshold * Const.NanoOrder)
      .map { value =>
        val inputAddresses       = tx.inputs.iterator.map(_.boxId).flatMap(utxoState.getAddressByUtxo).toSet
        val inputValueByAddress  = sumAddressValues(inputAddresses)
        val outputAddresses      = outputsWithoutPaybacksAndFees.iterator.map(_.address).toSet
        val outputValueByAddress = sumAddressValues(outputAddresses)
        val txMatch              = TxMatch(tx, value, inputValueByAddress, outputValueByAddress)
        Either.cond(
          inputValueByAddress.valuesIterator.map(_._1).sum >= value,
          txMatch,
          txMatch
        )
      }
      .flatMap {
        case Right(txMatch) =>
          Some(txMatch)
        case Left(txMatch) =>
          logger.info(s"Invalid $txMatch")
          None
      }
      .toList
  }

  def inspectNewBlock(
    newBlock: BestBlockInserted,
    utxoState: UtxoState,
    graphTraversalSource: GraphTraversalSource
  ): List[BlockMatch] = List.empty

}

object HighValueDetector {
  private val valueFormat = new DecimalFormat("#,###")

  case class BlockMatch(
    block: Block,
    blockValue: Value,
    inputs: Map[Address, Address.State],
    outputs: Map[Address, Address.State]
  )

  case class TxMatch(
    tx: ApiTransaction,
    txValue: Value,
    inputs: Map[Address, Address.State],
    outputs: Map[Address, Address.State]
  ) {

    override def toString: AlertMessage = {
      val fmtValue         = valueFormat.format(txValue / Const.NanoOrder)
      val fmtInputAddrSum  = valueFormat.format(inputs.valuesIterator.map(_._1).sum / Const.NanoOrder)
      val fmtOutputAddrSum = valueFormat.format(outputs.valuesIterator.map(_._1).sum / Const.NanoOrder)

      def stringify(tuple: (Address, Address.State)) = tuple match {
        case (address, Address.State(value, Some(Address.Stats(lastTxHeight, txCount, boxCount)))) =>
          val fmtVal = valueFormat.format(value / Const.NanoOrder)
          s"${address.asInstanceOf[String].take(5)} $fmtVal @ [$lastTxHeight/$txCount/$boxCount]"
        case (address, Address.State(value, None)) =>
          val fmtVal = valueFormat.format(value / Const.NanoOrder)
          s"${address.asInstanceOf[String].take(5)} $fmtVal"
      }

      val addressDetails =
        inputs.map(stringify).mkString("      totalValue @ [lastTxHeight/txCount/boxCount]", "\n", "\n===>\n") +
          outputs
            .map(stringify)
            .mkString("\n")
      val msg =
        s"${inputs.size} addresses with total of $fmtInputAddrSum Erg ===> $fmtValue Erg ===> ${outputs.size} addresses with total of $fmtOutputAddrSum Erg"
      s"https://explorer.ergoplatform.com/en/transactions/${tx.id}\n$msg\n$addressDetails"
    }

  }

}
