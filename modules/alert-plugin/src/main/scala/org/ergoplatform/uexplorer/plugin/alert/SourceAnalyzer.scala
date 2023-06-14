package org.ergoplatform.uexplorer.plugin.alert

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLWriter
import org.ergoplatform.uexplorer.{SortedTopAddressMap, Storage}
import org.ergoplatform.uexplorer.plugin.alert.HighValueDetector.{BlockMatch, TxMatch}

import java.io.FileOutputStream

class SourceAnalyzer extends Analyzer {

  def trackTx(
    txMatch: TxMatch,
    storage: Storage,
    graphTraversalSource: Option[GraphTraversalSource]
  ): Option[TxMatch] =
    // TODO currently lack of options for visualizing graphson/graphml besides https://gitlab.com/ouestware/retina
    Option(txMatch)

  def trackBlock(
    blockMatch: BlockMatch,
    storage: Storage,
    graphTraversalSource: Option[GraphTraversalSource]
  ): Option[BlockMatch] = Option.empty

}
