package org.ergoplatform.uexplorer.plugin.alert

import org.ergoplatform.uexplorer.{Address, BoxId}
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateWithPool, UtxoStateWithoutPool}
import org.ergoplatform.uexplorer.plugin.alert.Detector.AlertMessage

trait Detector {

  def inspect(
    newTx: ApiTransaction,
    utxoStateWoPool: UtxoStateWithoutPool,
    utxoStateWithPool: UtxoStateWithPool
  ): List[AlertMessage]

}

object Detector {
  type AlertMessage = String
}
