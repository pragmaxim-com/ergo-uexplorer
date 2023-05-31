package org.ergoplatform.uexplorer.plugin.alert

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.db.BestBlockInserted
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.alert.Detector.AlertMessage
import org.ergoplatform.uexplorer.plugin.alert.HighValueDetector.{BlockMatch, TxMatch}
import org.ergoplatform.uexplorer.utxo.UtxoState
import org.ergoplatform.uexplorer.{Address, BoxId, SortedTopAddressMap, TopAddressMap}

trait Detector {

  def inspectNewPoolTx(
    tx: ApiTransaction,
    utxoState: UtxoState,
    graphTraversalSource: GraphTraversalSource
  ): List[TxMatch]

  def inspectNewBlock(
    newBlock: BestBlockInserted,
    utxoState: UtxoState,
    graphTraversalSource: GraphTraversalSource
  ): List[BlockMatch]
}

object Detector {
  type AlertMessage = String
}
