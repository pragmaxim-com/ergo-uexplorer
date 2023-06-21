package org.ergoplatform.uexplorer.janusgraph

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.typesafe.scalalogging.LazyLogging
import eu.timepit.refined.auto.autoUnwrap
import org.apache.tinkerpop.gremlin.structure.{Direction, Graph, T, Vertex}
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.Const.*
import org.ergoplatform.uexplorer.db.{InputRecord, OutputRecord}

import scala.collection.immutable.{ArraySeq, ListMap}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

object TxGraphWriter extends LazyLogging {

  private val blackListBoxes = Set(Protocol.Emission.inputBox, Protocol.NoPremine.box, Protocol.Foundation.box)

  private val blackListAddresses =
    Set(FeeContract.address, Protocol.Emission.address, Protocol.NoPremine.address, Protocol.Foundation.address)

  def writeGraph(
    txId: TxId,
    height: Height,
    timestamp: Timestamp,
    inputs: Iterable[InputRecord],
    outputs: Iterable[OutputRecord]
  )(g: Graph): Unit = {
    val newTxVertex = g.addVertex(T.id, Utils.vertexHash(txId.unwrapped, g), T.label, "txId")
    newTxVertex.property("txId", txId)
    newTxVertex.property("height", height)
    newTxVertex.property("timestamp", timestamp)
    val inputsByAddress =
      inputs
        .filterNot(t =>
          blackListBoxes.contains(t.boxId) || blackListAddresses.contains(t.address) || t.value < CoinsInOneErgo
        )
        .groupBy(_.address)

    inputsByAddress
      .foreach { case (address, inputs) =>
        val inputAddressVertexIt = g.vertices(Utils.vertexHash(address, g))
        if (!inputAddressVertexIt.hasNext) {
          logger.error(s"inputAddress $address from height $height lacks corresponding vertex")
        }
        newTxVertex.addEdge("from", inputAddressVertexIt.next(), "value", 0L) // TODO inputs.iterator.map(_.value).sum
      }

    outputs
      .filterNot(t =>
        inputsByAddress.contains(t.address) || blackListBoxes.contains(t.boxId) || blackListAddresses.contains(
          t.address
        ) || t.value < CoinsInOneErgo
      )
      .groupBy(_.address)
      .foreach { case (address, outputs) =>
        val outputAddressVertexIt = g.vertices(Utils.vertexHash(address, g))
        val newOutputAddressVertex =
          if (outputAddressVertexIt.hasNext) {
            outputAddressVertexIt.next()
          } else {
            val newOutputAddressVertex = g.addVertex(T.id, Utils.vertexHash(address, g), T.label, "address")
            newOutputAddressVertex.property("address", address)
            newOutputAddressVertex
          }
        newTxVertex.addEdge("to", newOutputAddressVertex, "value", outputs.iterator.map(_.value).sum)
      }
  }

}
