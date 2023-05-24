package org.ergoplatform.uexplorer.janusgraph

import org.janusgraph.graphdb.database.StandardJanusGraph
import org.apache.commons.codec.digest.MurmurHash2
import org.apache.tinkerpop.gremlin.structure.Graph
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx

object Utils {

  private def hashAddress(address: String) = Math.abs(MurmurHash2.hash64(address)) / 1000

  def vertexHash(address: String, g: Graph): Long =
    g match {
      case graph if graph.isInstanceOf[StandardJanusGraph] =>
        graph.asInstanceOf[StandardJanusGraph].getIDManager.toVertexId(hashAddress(address))
      case graph if graph.isInstanceOf[StandardJanusGraphTx] =>
        graph.asInstanceOf[StandardJanusGraphTx].getGraph.getIDManager.toVertexId(hashAddress(address))
      case graph =>
        throw IllegalStateException(s"$graph currently not supported !!")

    }

}
