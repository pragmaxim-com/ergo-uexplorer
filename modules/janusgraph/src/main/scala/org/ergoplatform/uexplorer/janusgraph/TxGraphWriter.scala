package org.ergoplatform.uexplorer.janusgraph

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.gremlin.structure.{Direction, Graph, T, Vertex}
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.Const.*
import org.ergoplatform.uexplorer.db.{InputRecords, Utxo}
import org.ergoplatform.uexplorer.HexString.unwrapped

import scala.collection.immutable.{ArraySeq, ListMap}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

object TxGraphWriter extends LazyLogging {

  private val blackListBoxes = Set(Protocol.Emission.inputBox, Protocol.NoPremine.box, Protocol.Foundation.inputBox)

  private val blackListAddresses =
    Set(
      Protocol.FeeContract.ergoTreeHash,
      Protocol.Emission.ergoTreeHash,
      Protocol.NoPremine.ergoTreeHash,
      Protocol.Foundation.ergoTreeHash
    )

  def writeGraph(
    txId: TxId,
    height: Height,
    timestamp: Timestamp,
    inputsByErgoTree: mutable.Map[ErgoTreeHash, mutable.Map[BoxId, Value]],
    outputs: Iterable[Utxo]
  )(g: Graph): Unit = {
    val newTxVertex = g.addVertex(T.id, Utils.vertexHash(txId.unwrapped, g), T.label, "txId")
    newTxVertex.property("txId", txId)
    newTxVertex.property("height", height)
    newTxVertex.property("timestamp", timestamp)
    inputsByErgoTree
      .foreach { case (ergoTree, inputs) =>
        if (!blackListAddresses.contains(ergoTree)) {
          val inputValueSum =
            inputs.collect { // not necessary as we already filter out corresponding ergoTrees
              case (boxId, value) if !blackListBoxes.contains(boxId) && !(value < CoinsInOneErgo) => value
            }.sum

          val inputAddressVertexIt = g.vertices(Utils.vertexHash(ergoTree.unwrapped, g))
          if (!inputAddressVertexIt.hasNext) {
            logger.error(s"inputAddress $ergoTree from height $height lacks corresponding vertex")
          }
          newTxVertex.addEdge("from", inputAddressVertexIt.next(), "value", inputValueSum)
        }
      }

    outputs
      .filterNot(t =>
        inputsByErgoTree.contains(t.ergoTreeHash) || blackListBoxes.contains(t.boxId) || blackListAddresses.contains(
          t.ergoTreeHash
        ) || t.ergValue < CoinsInOneErgo
      )
      .groupBy(_.ergoTreeHash)
      .foreach { case (ergoTree, outputs) =>
        val outputAddressVertexIt = g.vertices(Utils.vertexHash(ergoTree.unwrapped, g))
        val newOutputAddressVertex =
          if (outputAddressVertexIt.hasNext) {
            outputAddressVertexIt.next()
          } else {
            val newOutputAddressVertex = g.addVertex(T.id, Utils.vertexHash(ergoTree.unwrapped, g), T.label, "address")
            newOutputAddressVertex.property("address", ergoTree)
            newOutputAddressVertex
          }
        newTxVertex.addEdge("to", newOutputAddressVertex, "value", outputs.iterator.map(_.ergValue).sum)
      }
  }

}
