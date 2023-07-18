package org.ergoplatform.uexplorer.plugin.alert

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.ReadableStorage
import org.ergoplatform.uexplorer.db.BestBlockInserted
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.alert.Detector.AlertMessage
import org.ergoplatform.uexplorer.plugin.alert.HighValueDetector.{BlockMatch, TxMatch}

trait Detector {

  def inspectNewPoolTx(
    tx: ApiTransaction,
    utxoState: ReadableStorage,
    graphTraversalSource: GraphTraversalSource
  ): List[TxMatch]

  def inspectNewBlock(
    newBlock: BestBlockInserted,
    utxoState: ReadableStorage,
    graphTraversalSource: GraphTraversalSource
  ): List[BlockMatch]
}

object Detector {
  type AlertMessage = String
}
