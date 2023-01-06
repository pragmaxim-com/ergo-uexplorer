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
import org.janusgraph.graphdb.database.StandardJanusGraph
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx

import scala.concurrent.Future

trait JanusGraphWriter {
  this: CassandraBackend =>

  private def addOutputVertices(epochIndex: Int)(implicit g: StandardJanusGraphTx) =
    Flow.fromFunction[(Tx, (ArraySeq[(BoxId, Address, Long)], ArraySeq[(BoxId, Address, Long)])), Unit] {
      case (tx, (inputs, outputs)) =>
        TxGraphWriter.writeGraph(tx, epochIndex, inputs, outputs)
    }

  def graphWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed] =
    Flow[(Block, Option[MaybeNewEpoch])]
      .mapAsync(1) {
        case (b, s @ Some(NewEpochDetected(e, boxesByHeight))) =>
          val threadedGraph = janusGraph.tx().createThreadedTx[StandardJanusGraphTx]()
          Source
            .fromIterator(() => boxesByHeight.iterator.flatMap(_._2))
            .via(
              cpuHeavyBalanceFlow(addOutputVertices(e.index)(threadedGraph), Runtime.getRuntime.availableProcessors() / 4)
            )
            .run()
            .map { _ =>
              threadedGraph.tx().commit()
              logger.info(s"New epoch ${e.index} building finished")
              b -> s
            }
        case x => Future.successful(x)
      }
}
