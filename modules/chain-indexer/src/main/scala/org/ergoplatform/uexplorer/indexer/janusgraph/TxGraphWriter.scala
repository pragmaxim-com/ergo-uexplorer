package org.ergoplatform.uexplorer.indexer.janusgraph

import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.gremlin.structure.{Direction, Graph, T, Vertex}
import org.ergoplatform.uexplorer.indexer.Utils
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState.Tx
import org.ergoplatform.uexplorer.{Address, BoxId, Const, TxId}
import org.janusgraph.graphdb.database.StandardJanusGraph
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx
import eu.timepit.refined.auto.autoUnwrap
import Const.*
import scala.jdk.CollectionConverters.*
import scala.collection.immutable.ArraySeq

object TxGraphWriter extends LazyLogging {

  private val blackListBoxes = Set(Genesis.Emission.box, Genesis.NoPremine.box, Genesis.Foundation.box)

  private val blackListAddresses =
    Set(FeeContract.address, Genesis.Emission.address, Genesis.NoPremine.address, Genesis.Foundation.address)

  def writeGraph(
    tx: Tx,
    epochIndex: Int,
    inputs: ArraySeq[(BoxId, Address, Long)],
    outputs: ArraySeq[(BoxId, Address, Long)]
  )(implicit graph: StandardJanusGraphTx): Unit = {
    val newTxVertex = graph.addVertex(T.id, Utils.vertexHash(tx.id.unwrapped))
    newTxVertex.property("height", tx.height)
    newTxVertex.property("timestamp", tx.timestamp)
    val inputsByAddress =
      inputs
        .filterNot(t => blackListBoxes.contains(t._1) || blackListAddresses.contains(t._2) || t._3 < CoinsInOneErgo)
        .groupBy(_._2)
        .view
        .mapValues(_.map(t => t._1 -> t._3))
        .toMap

    val relatedTxVertices =
      inputsByAddress
        .map { case (address, valueByBoxId) =>
          val inputAddressVertexIt = graph.vertices(Utils.vertexHash(address))
          if (!inputAddressVertexIt.hasNext) {
            logger.error(s"inputAddress $address from epoch $epochIndex lacks corresponding vertex")
          }
          (inputAddressVertexIt.next(), valueByBoxId)
        }
        .flatMap { case (inputAddressVertex, valueByBoxId) =>
          newTxVertex.addEdge("from", inputAddressVertex, "value", valueByBoxId.map(_._2).sum)
          inputAddressVertex
            .edges(Direction.IN, "to")
            .asScala
            .map(_.inVertex())
        }
    val relatedTxCount = relatedTxVertices.size
    if (relatedTxCount > 1000) {
      logger.warn(s"$relatedTxCount related transactions with addresses:${inputsByAddress.mkString("\n", "\n", "")}")
    }

    relatedTxVertices
      .foreach { relatedTx =>
        newTxVertex.addEdge("relatedTo", relatedTx)
      }

    val inputAddresses = inputsByAddress.keySet
    outputs
      .filterNot(t =>
        inputAddresses.contains(t._2) || blackListBoxes.contains(t._1) || blackListAddresses.contains(
          t._2
        ) || t._3 < CoinsInOneErgo
      )
      .groupBy(_._2)
      .view
      .mapValues(_.map(t => t._1 -> t._3))
      .map { case (address, valueByBox) =>
        val outputAddressVertexIt = graph.vertices(Utils.vertexHash(address))
        if (outputAddressVertexIt.hasNext) {
          outputAddressVertexIt.next() -> valueByBox
        } else {
          graph.addVertex(T.id, Utils.vertexHash(address)) -> valueByBox
        }
      }
      .foreach { case (outputAddressVertex, valueByBoxId) =>
        newTxVertex.addEdge("to", outputAddressVertex, "value", valueByBoxId.map(_._2).sum)
      }
  }
}
