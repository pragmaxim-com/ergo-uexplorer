package org.ergoplatform.uexplorer.plugin.alert

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.ReadableStorage
import org.ergoplatform.uexplorer.plugin.alert.HighValueDetector.{BlockMatch, TxMatch}

trait Analyzer {

  def trackTx(
    txMatch: TxMatch,
    utxoState: ReadableStorage,
    graphTraversalSource: GraphTraversalSource
  ): Option[TxMatch]

  def trackBlock(
    blockMatch: BlockMatch,
    utxoState: ReadableStorage,
    graphTraversalSource: GraphTraversalSource
  ): Option[BlockMatch]

}
