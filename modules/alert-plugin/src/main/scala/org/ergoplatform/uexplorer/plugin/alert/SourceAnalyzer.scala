package org.ergoplatform.uexplorer.plugin.alert

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLWriter
import org.ergoplatform.uexplorer.SortedTopAddressMap
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateWithPool, UtxoStateWithoutPool}
import org.ergoplatform.uexplorer.plugin.alert.HighValueDetector.{BlockMatch, TxMatch}

import java.io.FileOutputStream

class SourceAnalyzer extends Analyzer {

  def trackTx(
    txMatch: TxMatch,
    utxoStateWoPool: UtxoStateWithoutPool,
    utxoStateWithPool: UtxoStateWithPool,
    graphTraversalSource: GraphTraversalSource,
    topAddresses: SortedTopAddressMap
  ): Option[TxMatch] =
    /*
    val subGraph =
      graphTraversalSource
        .V(txMatch.inputs.keys)
        .out("knows")
        .out("created")
        .values("property")
        .next()

    val stream = new FileOutputStream("data/jupiter.xml")
    GraphMLWriter.build().vertexLabelKey("labels").create().writeGraph(stream, subGraph)
     */
    Option(txMatch)

  def trackBlock(
    blockMatch: BlockMatch,
    utxoStateWoPool: UtxoStateWithoutPool,
    graphTraversalSource: GraphTraversalSource,
    topAddresses: SortedTopAddressMap
  ): Option[BlockMatch] = Option.empty

}
