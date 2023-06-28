package org.ergoplatform.uexplorer.indexer.chain

import akka.actor.typed.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.gremlin.structure.Graph
import org.ergoplatform.uexplorer.db.Backend
import org.ergoplatform.uexplorer.indexer.chain.Initializer.*
import org.ergoplatform.uexplorer.indexer.db.Backend
import org.ergoplatform.uexplorer.janusgraph.api.GraphBackend
import org.ergoplatform.uexplorer.storage.MvStorage
import org.ergoplatform.uexplorer.{BlockId, Const, Height}

import java.nio.file.{Files, Paths}
import scala.collection.immutable.TreeSet
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

class Initializer(
  storage: MvStorage,
  backend: Backend,
  graphBackendOpt: Option[GraphBackend]
) extends LazyLogging {

  def init: Future[ChainIntegrity] =
    backend.isEmpty.map { backendEmpty =>
      if (storage.isEmpty && !backendEmpty) {
        HalfEmptyInconsistency("Backend must be empty when storage is.")
      } else if (!storage.isEmpty && backendEmpty) {
        HalfEmptyInconsistency(s"Storage must be empty when backend is.")
      } else if (storage.isEmpty && backendEmpty) {
        if (graphBackendOpt.exists(_.initGraph) || graphBackendOpt.isEmpty) {
          logger.info(s"Chain is empty, loading from scratch ...")
          ChainEmpty
        } else {
          GraphInconsistency("Janus graph must be empty when main db is empty, drop janusgraph keyspace!")
        }
      } else {
        if (graphBackendOpt.isEmpty || graphBackendOpt.exists(g => !g.isEmpty))
          ChainValid
        else
          GraphInconsistency("Janus graph cannot be empty when main db is not")
      }
    }
}

object Initializer {

  trait ChainIntegrity
  case class HalfEmptyInconsistency(error: String) extends ChainIntegrity
  case class GraphInconsistency(error: String) extends ChainIntegrity
  case object ChainEmpty extends ChainIntegrity
  case object ChainValid extends ChainIntegrity

}
