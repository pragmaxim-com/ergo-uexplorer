package org.ergoplatform.uexplorer.janusgraph

import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.Flow
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Transaction
import org.ergoplatform.uexplorer.Height
import org.ergoplatform.uexplorer.db.FullBlock
import org.ergoplatform.uexplorer.janusgraph.api.GraphBackend
import org.janusgraph.core.JanusGraphFactory
import org.janusgraph.graphdb.database.StandardJanusGraph

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

class JanusGraphBackend(val janusGraph: StandardJanusGraph) extends GraphBackend with JanusGraphWriter {

  def graphTraversalSource: GraphTraversalSource = janusGraph.traversal()

  def tx: Transaction = janusGraph.tx()

  def isEmpty: Boolean = janusGraph.traversal().V().hasNext

  def close(): Future[Unit] =
    Future.fromTry(Try(janusGraph.close()))

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
