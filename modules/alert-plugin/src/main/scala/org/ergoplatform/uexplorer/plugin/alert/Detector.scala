package org.ergoplatform.uexplorer.plugin.alert

import org.ergoplatform.uexplorer.{Address, BoxId}
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.alert.Detector.AlertMessage

trait Detector {

  def inspect(
    tx: ApiTransaction,
    addressByUtxo: Map[BoxId, Address],
    utxosByAddress: Map[Address, Map[BoxId, Long]]
  ): List[AlertMessage]

}

object Detector {
  type AlertMessage = String
}
