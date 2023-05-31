package org.ergoplatform.uexplorer.plugin.alert

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.SortedTopAddressMap
import org.ergoplatform.uexplorer.plugin.alert.HighValueDetector.{BlockMatch, TxMatch}
import org.ergoplatform.uexplorer.utxo.UtxoState

trait Analyzer {

  def trackTx(
    txMatch: TxMatch,
    utxoState: UtxoState,
    graphTraversalSource: GraphTraversalSource
  ): Option[TxMatch]

  def trackBlock(
    blockMatch: BlockMatch,
    utxoState: UtxoState,
    graphTraversalSource: GraphTraversalSource
  ): Option[BlockMatch]

}
