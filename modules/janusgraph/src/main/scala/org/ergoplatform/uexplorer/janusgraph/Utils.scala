package org.ergoplatform.uexplorer.janusgraph

import org.janusgraph.graphdb.database.StandardJanusGraph
import org.apache.commons.codec.digest.MurmurHash2
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx

object Utils {

  def vertexHash(address: String, g: StandardJanusGraph): Long =
    g.getIDManager.toVertexId(Math.abs(MurmurHash2.hash64(address)) / 1000)

  def vertexHash(address: String)(implicit tx: StandardJanusGraphTx): Long =
    vertexHash(address, tx.getGraph)
}
