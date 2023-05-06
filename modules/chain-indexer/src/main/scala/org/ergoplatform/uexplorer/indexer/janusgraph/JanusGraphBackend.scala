package org.ergoplatform.uexplorer.indexer.janusgraph

import akka.{Done, NotUsed}
import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.Flow
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.api.GraphBackend
import org.ergoplatform.uexplorer.indexer.chain.ChainStateHolder.MaybeNewEpoch
import org.ergoplatform.uexplorer.indexer.janusgraph
import org.janusgraph.core.JanusGraphFactory
import org.janusgraph.graphdb.database.StandardJanusGraph

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

class JanusGraphBackend(val janusGraph: StandardJanusGraph) extends GraphBackend with JanusGraphWriter {

  def graphTraversalSource: GraphTraversalSource = janusGraph.traversal()

  def isEmpty: Boolean = janusGraph.traversal().V().hasNext

  def close(): Future[Unit] = Future(janusGraph.close())

}

object JanusGraphBackend extends LazyLogging {

  def apply()(implicit system: ActorSystem[Nothing]): Try[JanusGraphBackend] = {
    val datastaxDriverConf = system.settings.config.getConfig("datastax-java-driver")
    Try {
      val janusGraph = JanusGraphFactory.build
        .set("storage.backend", "cql")
        .set("storage.hostname", datastaxDriverConf.getStringList("basic.contact-points").get(0))
        .set("graph.set-vertex-id", true)
        .open()
        .asInstanceOf[StandardJanusGraph]

      logger.info(s"Janus graph created")
      val backend = new JanusGraphBackend(janusGraph)
      CoordinatedShutdown(system).addTask(
        CoordinatedShutdown.PhaseServiceStop,
        "stop-janus-graph-backend"
      ) { () =>
        backend.close().map { _ =>
          Done
        }
      }
      backend
    }
  }

}
