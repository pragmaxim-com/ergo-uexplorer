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
    Set(FeeContract.ergoTree, Protocol.Emission.ergoTree, Protocol.NoPremine.ergoTree, Protocol.Foundation.ergoTree)

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
    val inputsByErgoTrees =
      inputs
        .filterNot(t =>
          blackListBoxes.contains(t.boxId) || blackListAddresses.contains(t.ergoTree) || t.value < CoinsInOneErgo
        )
        .groupBy(_.ergoTree)

    inputsByErgoTrees
      .foreach { case (ergoTree, inputs) =>
        val inputAddressVertexIt = g.vertices(Utils.vertexHash(ergoTree, g))
        if (!inputAddressVertexIt.hasNext) {
          logger.error(s"inputAddress $ergoTree from height $height lacks corresponding vertex")
        }
        newTxVertex.addEdge("from", inputAddressVertexIt.next(), "value", inputs.iterator.map(_.value).sum)
      }

    outputs
      .filterNot(t =>
        inputsByErgoTrees.contains(t.ergoTree) || blackListBoxes.contains(t.boxId) || blackListAddresses.contains(
          t.ergoTree
        ) || t.value < CoinsInOneErgo
      )
      .groupBy(_.ergoTree)
      .foreach { case (ergoTree, outputs) =>
        val outputAddressVertexIt = g.vertices(Utils.vertexHash(ergoTree, g))
        val newOutputAddressVertex =
          if (outputAddressVertexIt.hasNext) {
            outputAddressVertexIt.next()
          } else {
            val newOutputAddressVertex = g.addVertex(T.id, Utils.vertexHash(ergoTree, g), T.label, "address")
            newOutputAddressVertex.property("address", ergoTree)
            newOutputAddressVertex
          }
        newTxVertex.addEdge("to", newOutputAddressVertex, "value", outputs.iterator.map(_.value).sum)
      }
  }

}
