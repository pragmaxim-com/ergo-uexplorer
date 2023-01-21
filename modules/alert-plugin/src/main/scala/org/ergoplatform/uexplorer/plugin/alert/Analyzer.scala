package org.ergoplatform.uexplorer.plugin.alert

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateWithPool, UtxoStateWithoutPool}
import org.ergoplatform.uexplorer.plugin.alert.HighValueDetector.{BlockMatch, TxMatch}
import org.ergoplatform.uexplorer.SortedTopAddressMap

trait Analyzer {

  def trackTx(
    txMatch: TxMatch,
    utxoStateWoPool: UtxoStateWithoutPool,
    utxoStateWithPool: UtxoStateWithPool,
    topAddresses: SortedTopAddressMap,
    graphTraversalSource: GraphTraversalSource
  ): Option[TxMatch]

  def trackBlock(
    blockMatch: BlockMatch,
    utxoStateWoPool: UtxoStateWithoutPool,
    topAddresses: SortedTopAddressMap,
    graphTraversalSource: GraphTraversalSource
  ): Option[BlockMatch]

}
