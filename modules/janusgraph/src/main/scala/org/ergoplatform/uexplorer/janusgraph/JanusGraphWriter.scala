package org.ergoplatform.uexplorer.janusgraph

import akka.NotUsed
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, Source}
import com.typesafe.scalalogging.LazyLogging
import eu.timepit.refined.auto.autoUnwrap
import org.apache.tinkerpop.gremlin.structure.{Graph, T, Vertex}
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.db.{BestBlockInserted, BlockWithInputs, FullBlock}
import org.janusgraph.core.Multiplicity

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

  def writeTxsAndCommit(blocks: Seq[BestBlockInserted]): IterableOnce[BestBlockInserted] = {
    blocks.iterator
      .foreach { case BestBlockInserted(b, _) =>
        b.inputRecords.groupBy(_.txId).foreach { case (txId, inputRecords) =>
          val outputRecords = b.outputRecords.filter(_.txId == txId)
          TxGraphWriter.writeGraph(txId, b.info.height, b.info.timestamp, inputRecords, outputRecords)(janusGraph)

        }
      }
    janusGraph.tx().commit()
    blocks
  }

  def graphWriteFlow: Flow[BestBlockInserted, BestBlockInserted, NotUsed] =
    Flow[BestBlockInserted]
      .grouped(32)
      .mapAsync(1) { blocks =>
        Future(writeTxsAndCommit(blocks))
      }
      .mapConcat(identity)
}
