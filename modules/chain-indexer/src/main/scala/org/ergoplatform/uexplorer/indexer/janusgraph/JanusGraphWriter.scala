package org.ergoplatform.uexplorer.indexer.janusgraph

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import org.apache.tinkerpop.gremlin.structure.{Graph, T, Vertex}
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.Utils
import org.ergoplatform.uexplorer.{Address, BoxId, Const, TxId}
import org.ergoplatform.uexplorer.indexer.cassandra.CassandraBackend
import org.ergoplatform.uexplorer.indexer.chain.ChainStateHolder.{MaybeNewEpoch, NewEpochDetected}
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState.Tx

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.immutable.ArraySeq
import eu.timepit.refined.auto.autoUnwrap
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState
import org.janusgraph.core.Multiplicity
import org.janusgraph.graphdb.database.StandardJanusGraph
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx

import scala.concurrent.Future

trait JanusGraphWriter {
  this: CassandraBackend =>

  def initGraph: Boolean = {
    val mgmt = janusGraph.openManagement()
    if (!mgmt.containsEdgeLabel("relatedTo")) {
      logger.info("Creating Janus properties, indexes and labels")
      mgmt.makeEdgeLabel("from").unidirected().multiplicity(Multiplicity.SIMPLE).make()
      mgmt.makeEdgeLabel("to").multiplicity(Multiplicity.SIMPLE).make()
      mgmt.makeEdgeLabel("relatedTo").multiplicity(Multiplicity.SIMPLE).make()
      mgmt.makePropertyKey("height").dataType(classOf[Integer]).make()
      mgmt.makePropertyKey("timestamp").dataType(classOf[java.lang.Long]).make()
      mgmt.makePropertyKey("value").dataType(classOf[java.lang.Long]).make()
      mgmt.commit()
      true
    } else {
      logger.info("Janus graph already initialized...")
      false
    }
  }

  def graphWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed] =
    Flow[(Block, Option[MaybeNewEpoch])]
      .mapAsync(1) {
        case (b, s @ Some(NewEpochDetected(e, boxesByHeight))) =>
          val txGraphWriter = TxGraphWriter(janusGraph)
          Source
            .fromIterator(() => boxesByHeight.iterator)
            .via(
              cpuHeavyBalanceFlow(txGraphWriter.graphTxWriteFlow, Runtime.getRuntime.availableProcessors() / 4)
            )
            .run()
            .map { _ =>
              txGraphWriter.commit()
              logger.info(s"New epoch ${e.index} building finished")
              b -> s
            }
        case x => Future.successful(x)
      }
}
