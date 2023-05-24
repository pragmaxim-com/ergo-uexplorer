package org.ergoplatform.uexplorer.janusgraph

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.typesafe.scalalogging.LazyLogging
import eu.timepit.refined.auto.autoUnwrap
import org.apache.tinkerpop.gremlin.structure.{Direction, Graph, T, Vertex}
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.Const.*

import scala.collection.immutable.{ArraySeq, ListMap}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

object TxGraphWriter extends LazyLogging {

  private val blackListBoxes = Set(Genesis.Emission.box, Genesis.NoPremine.box, Genesis.Foundation.box)

  private val blackListAddresses =
    Set(FeeContract.address, Genesis.Emission.address, Genesis.NoPremine.address, Genesis.Foundation.address)

  def writeGraph(
    tx: Tx,
    height: Int,
    inputs: ArraySeq[(BoxId, Address, Long)],
    outputs: ArraySeq[(BoxId, Address, Long)],
    topAddresses: TopAddressMap
  )(g: Graph): Unit = {
    val newTxVertex = g.addVertex(T.id, Utils.vertexHash(tx.id.unwrapped, g), T.label, "txId")
    newTxVertex.property("txId", tx.id)
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
        val inputAddressVertexIt = g.vertices(Utils.vertexHash(address, g))
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
        val outputAddressVertexIt = g.vertices(Utils.vertexHash(address, g))
        if (outputAddressVertexIt.hasNext) {
          outputAddressVertexIt.next() -> valueByBox
        } else {
          val newOutputAddressVertex = g.addVertex(T.id, Utils.vertexHash(address, g), T.label, "address")
          newOutputAddressVertex.property("address", address)
          newOutputAddressVertex -> valueByBox
        }
      }
      .foreach { case (outputAddressVertex, valueByBoxId) =>
        newTxVertex.addEdge("to", outputAddressVertex, "value", valueByBoxId.map(_._2).sum)
      }
  }

}
