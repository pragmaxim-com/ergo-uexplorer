package org.ergoplatform.uexplorer.plugin

import org.ergoplatform.uexplorer.{Address, BoxId, TxId}
import org.ergoplatform.uexplorer.node.ApiTransaction

import scala.concurrent.Future
import scala.util.Try

trait Plugin {

  def name: String

  def init: Try[Unit]

  def execute(
    newMempoolTxs: Map[TxId, ApiTransaction],
    addressByUtxo: Map[BoxId, Address],
    utxosByAddress: Map[Address, Map[BoxId, Long]]
  ): Future[Unit]
}
