package org.ergoplatform.uexplorer.janusgraph

import akka.NotUsed
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, Source}
import com.typesafe.scalalogging.LazyLogging
import eu.timepit.refined.auto.autoUnwrap
import org.apache.tinkerpop.gremlin.structure.{Graph, T, Vertex}
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.*
import org.janusgraph.core.Multiplicity
import org.ergoplatform.uexplorer.Epoch.{EpochCommand, IgnoreEpoch, WriteNewEpoch}
import scala.collection.immutable.{ArraySeq, TreeMap}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait JanusGraphWriter extends LazyLogging {
  this: JanusGraphBackend =>

  def initGraph: Boolean = {
    val mgmt = janusGraph.openManagement()
    if (!mgmt.containsEdgeLabel("from")) {
      logger.info("Creating Janus properties, indexes and labels")
      // tx -> address edges
      mgmt.makeEdgeLabel("from").unidirected().multiplicity(Multiplicity.SIMPLE).make()
      mgmt.makeEdgeLabel("to").multiplicity(Multiplicity.SIMPLE).make()
      mgmt.makePropertyKey("value").dataType(classOf[java.lang.Long]).make()

      // addresses
      mgmt.makeVertexLabel("address").make()
      mgmt.makePropertyKey("address").dataType(classOf[String]).make()

      // transactions
      mgmt.makeVertexLabel("txId").make()
      mgmt.makePropertyKey("txId").dataType(classOf[String]).make()
      mgmt.makePropertyKey("height").dataType(classOf[Integer]).make()
      mgmt.makePropertyKey("timestamp").dataType(classOf[java.lang.Long]).make()
      mgmt.commit()
      true
    } else {
      logger.info("Janus graph already initialized...")
      false
    }
  }

  def writeTx(height: Height, boxesByTx: BoxesByTx, topAddresses: TopAddressMap, g: Graph): Unit =
    boxesByTx.foreach { case (tx, (inputs, outputs)) =>
      TxGraphWriter.writeGraph(tx, height, inputs, outputs, topAddresses)(g)
    }

  def writeTxsAndCommit(txBoxesByHeight: IterableOnce[(Height, BoxesByTx)], topAddresses: TopAddressMap): Unit = {
    txBoxesByHeight.iterator
      .foreach { case (height, boxesByTx) =>
        writeTx(height, boxesByTx, topAddresses, janusGraph)
      }
    janusGraph.tx().commit()
  }

  def graphWriteFlow: Flow[(Block, Option[EpochCommand]), (Block, Option[EpochCommand]), NotUsed] =
    Flow[(Block, Option[EpochCommand])]
      .mapAsync(1) {
        case (b, s @ Some(WriteNewEpoch(e, boxesByHeight, topAddresses))) =>
          Future {
            writeTxsAndCommit(boxesByHeight, topAddresses)
            logger.info(s"New epoch ${e.index} graph building finished")
            b -> s
          }
        case x =>
          Future.successful(x)
      }
}
