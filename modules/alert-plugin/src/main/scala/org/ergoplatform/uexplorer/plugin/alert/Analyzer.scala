package org.ergoplatform.uexplorer.plugin.alert

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.Storage
import org.ergoplatform.uexplorer.plugin.alert.HighValueDetector.{BlockMatch, TxMatch}

trait Analyzer {

  def trackTx(
    txMatch: TxMatch,
    utxoState: Storage,
    graphTraversalSource: Option[GraphTraversalSource]
  ): Option[TxMatch]

  def trackBlock(
    blockMatch: BlockMatch,
    utxoState: Storage,
    graphTraversalSource: Option[GraphTraversalSource]
  ): Option[BlockMatch]

}
