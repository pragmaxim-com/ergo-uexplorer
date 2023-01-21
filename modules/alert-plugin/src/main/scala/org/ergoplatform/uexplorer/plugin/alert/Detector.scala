package org.ergoplatform.uexplorer.plugin.alert

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.{Address, BoxId, SortedTopAddressMap, TopAddressMap}
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateWithPool, UtxoStateWithoutPool}
import org.ergoplatform.uexplorer.plugin.alert.Detector.AlertMessage
import org.ergoplatform.uexplorer.plugin.alert.HighValueDetector.TxMatch
import org.ergoplatform.uexplorer.plugin.alert.HighValueDetector.BlockMatch

trait Detector {

  def inspectNewPoolTx(
    tx: ApiTransaction,
    utxoStateWoPool: UtxoStateWithoutPool,
    utxoStateWithPool: UtxoStateWithPool,
    topAddresses: SortedTopAddressMap,
    graphTraversalSource: GraphTraversalSource
  ): List[TxMatch]

  def inspectNewBlock(
    newBlock: Block,
    utxoStateWoPool: UtxoStateWithoutPool,
    topAddresses: SortedTopAddressMap,
    graphTraversalSource: GraphTraversalSource
  ): List[BlockMatch]
}

object Detector {
  type AlertMessage = String
}
