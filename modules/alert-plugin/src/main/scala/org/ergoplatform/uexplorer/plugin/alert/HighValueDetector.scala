package org.ergoplatform.uexplorer.plugin.alert

import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.alert.Detector.AlertMessage
import HighValueDetector.*
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.db.{BestBlockInserted, FullBlock}
import org.slf4j.{Logger, LoggerFactory}

import java.text.DecimalFormat
import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters.*

class HighValueDetector(txErgValueThreshold: Long, blockErgValueThreshold: Long) extends Detector {

  private val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def inspectNewPoolTx(
    tx: ApiTransaction,
    storage: ReadableStorage,
    graphTraversalSource: GraphTraversalSource
  ): List[TxMatch] = {
    def sumErgoTreeValues(ergoTrees: Set[ErgoTreeHex]): Map[ErgoTreeHex, Value] = Map.empty
    // TODO this needs utxosByErgoTreeHex to become MultiMap again :-D
    /*
      ergoTrees
        .flatMap(a => storage.getUtxosByErgoTreeHex(a).map(a -> _))
        .foldLeft(Map.empty[ErgoTreeHex, Value]) { case (acc, (ergoTree, valueByBox)) =>
          acc.updated(ergoTree, valueByBox.values.asScala.sum)
        }
     */

    val outputsWithoutPaybacksAndFees =
      tx.outputs.filterNot(o =>
        tx.inputs.exists(i =>
          storage.getErgoTreeHexByUtxo(i.boxId).contains(o.ergoTree)
        ) || o.ergoTree == Const.Protocol.FeeContract.address
      )
    Option(outputsWithoutPaybacksAndFees.iterator.map(_.value).sum)
      .filter(_ >= txErgValueThreshold * Const.NanoOrder)
      .map { value =>
        val inputErgoTrees        = tx.inputs.iterator.map(_.boxId).flatMap(storage.getErgoTreeHexByUtxo).toSet
        val inputValueByErgoTree  = sumErgoTreeValues(inputErgoTrees)
        val outputErgoTrees       = outputsWithoutPaybacksAndFees.iterator.map(_.ergoTree).toSet
        val outputValueByErgoTree = sumErgoTreeValues(outputErgoTrees)
        val txMatch               = TxMatch(tx, value, inputValueByErgoTree, outputValueByErgoTree)
        Either.cond(
          inputValueByErgoTree.valuesIterator.sum >= value,
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
    storage: ReadableStorage,
    graphTraversalSource: GraphTraversalSource
  ): List[BlockMatch] = List.empty

}

object HighValueDetector {
  private val valueFormat = new DecimalFormat("#,###")

  case class BlockMatch(
    block: FullBlock,
    blockValue: Value,
    inputs: Map[ErgoTreeHex, Value],
    outputs: Map[ErgoTreeHex, Value]
  )

  case class TxMatch(
    tx: ApiTransaction,
    txValue: Value,
    inputs: Map[ErgoTreeHex, Value],
    outputs: Map[ErgoTreeHex, Value]
  ) {

    override def toString: AlertMessage = {
      val fmtValue         = valueFormat.format(txValue / Const.NanoOrder)
      val fmtInputAddrSum  = valueFormat.format(inputs.valuesIterator.sum / Const.NanoOrder)
      val fmtOutputAddrSum = valueFormat.format(outputs.valuesIterator.sum / Const.NanoOrder)

      def stringify(tuple: (ErgoTreeHex, Value)) = tuple match {
        case (ergoTree, value) =>
          val fmtVal = valueFormat.format(value / Const.NanoOrder)
          s"${ergoTree.asInstanceOf[String].take(5)} $fmtVal"
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
