package org.ergoplatform.uexplorer.indexer.janusgraph

import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.gremlin.structure.{Direction, Graph, T, Vertex}
import org.ergoplatform.uexplorer.indexer.Utils
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState.Tx
import org.ergoplatform.uexplorer.{Address, BoxId, Const, Height, TxId}
import org.janusgraph.graphdb.database.StandardJanusGraph
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx
import eu.timepit.refined.auto.autoUnwrap
import Const.*
import akka.NotUsed
import akka.stream.scaladsl.Flow
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState

import scala.jdk.CollectionConverters.*
import scala.collection.immutable.ArraySeq

class TxGraphWriter(implicit g: StandardJanusGraphTx) extends LazyLogging {

  private val blackListBoxes = Set(Genesis.Emission.box, Genesis.NoPremine.box, Genesis.Foundation.box)

  private val blackListAddresses =
    Set(FeeContract.address, Genesis.Emission.address, Genesis.NoPremine.address, Genesis.Foundation.address)

  val graphTxWriteFlow: Flow[(Height, UtxoState.BoxesByTx), (Height, UtxoState.BoxesByTx), NotUsed] =
    Flow.fromFunction[(Height, UtxoState.BoxesByTx), (Height, UtxoState.BoxesByTx)] { case (height, boxesByTx) =>
      boxesByTx.foreach { case (tx, (inputs, outputs)) => writeGraph(tx, height, inputs, outputs) }
      (height, boxesByTx)
    }

  def commit(): Unit = g.commit()

  private def writeGraph(
    tx: Tx,
    height: Int,
    inputs: ArraySeq[(BoxId, Address, Long)],
    outputs: ArraySeq[(BoxId, Address, Long)]
  ): Unit = {
    val newTxVertex = g.addVertex(T.id, Utils.vertexHash(tx.id.unwrapped))
    newTxVertex.property("height", tx.height)
    newTxVertex.property("timestamp", tx.timestamp)
    val inputsByAddress =
      inputs
        .filterNot(t => blackListBoxes.contains(t._1) || blackListAddresses.contains(t._2) || t._3 < CoinsInOneErgo)
        .groupBy(_._2)
        .view
        .mapValues(_.map(t => t._1 -> t._3))
        .toMap

    inputsByAddress
      .map { case (address, valueByBoxId) =>
        val inputAddressVertexIt = g.vertices(Utils.vertexHash(address))
        if (!inputAddressVertexIt.hasNext) {
          logger.error(s"inputAddress $address from height $height lacks corresponding vertex")
        }
        (inputAddressVertexIt.next(), valueByBoxId)
      }
      .foreach { case (inputAddressVertex, valueByBoxId) =>
        newTxVertex.addEdge("from", inputAddressVertex, "value", valueByBoxId.map(_._2).sum)
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
        val outputAddressVertexIt = g.vertices(Utils.vertexHash(address))
        if (outputAddressVertexIt.hasNext) {
          outputAddressVertexIt.next() -> valueByBox
        } else {
          g.addVertex(T.id, Utils.vertexHash(address)) -> valueByBox
        }
      }
      .foreach { case (outputAddressVertex, valueByBoxId) =>
        newTxVertex.addEdge("to", outputAddressVertex, "value", valueByBoxId.map(_._2).sum)
      }
  }
}

object TxGraphWriter {

  def apply(janusGraph: StandardJanusGraph): TxGraphWriter = new TxGraphWriter()(
    janusGraph.tx().createThreadedTx[StandardJanusGraphTx]()
  )
}
