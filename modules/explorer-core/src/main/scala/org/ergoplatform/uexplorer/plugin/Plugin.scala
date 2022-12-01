package org.ergoplatform.uexplorer.plugin

import org.ergoplatform.uexplorer.{Address, BoxId, TxId}
import org.ergoplatform.uexplorer.node.ApiTransaction

import scala.collection.mutable
import scala.concurrent.Future

trait Plugin {

  def execute(
    newMempoolTxs: Map[TxId, ApiTransaction],
    addressByUtxo: Map[BoxId, Address],
    utxosByAddress: Map[Address, mutable.Map[BoxId, Long]]
  ): Future[Unit]
}
