package org.ergoplatform.uexplorer.plugin.alert

import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.{Address, BoxId}
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateWithPool, UtxoStateWithoutPool}
import org.ergoplatform.uexplorer.plugin.alert.Detector.AlertMessage

trait Detector {

  def inspectNewPoolTx(
    tx: ApiTransaction,
    utxoStateWoPool: UtxoStateWithoutPool,
    utxoStateWithPool: UtxoStateWithPool
  ): List[AlertMessage]

  def inspectNewBlock(
    newBlock: Block,
    utxoStateWoPool: UtxoStateWithoutPool
  ): List[AlertMessage]
}

object Detector {
  type AlertMessage = String
}
