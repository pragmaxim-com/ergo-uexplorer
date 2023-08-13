package org.ergoplatform.uexplorer.indexer.chain

import org.ergoplatform.uexplorer.ReadableStorage
import org.ergoplatform.uexplorer.backend.Repo
import org.ergoplatform.uexplorer.db.GraphBackend
import org.ergoplatform.uexplorer.indexer.chain.Initializer.*
import zio.{Task, ZLayer}

case class Initializer(
  storage: ReadableStorage,
  repo: Repo,
  graphBackend: GraphBackend
) {

  def init: Task[ChainIntegrity] =
    repo.isEmpty.map { backendEmpty =>
      if (storage.isEmpty && !backendEmpty) {
        HalfEmptyInconsistency("Backend must be empty when storage is.")
      } else if (!storage.isEmpty && backendEmpty) {
        HalfEmptyInconsistency(s"Storage must be empty when backend is.")
      } else if (storage.isEmpty && backendEmpty) {
        if (graphBackend.initGraph) {
          ChainEmpty
        } else {
          GraphInconsistency("Janus graph must be empty when main db is empty, drop janusgraph keyspace!")
        }
      } else {
        ChainValid
        /*
        if (!graphBackend.isEmpty)
          ChainValid
        else
          GraphInconsistency("Janus graph cannot be empty when main db is not")
         */
      }
    }
}

object Initializer {

  def layer: ZLayer[ReadableStorage with Repo with GraphBackend, Nothing, Initializer] =
    ZLayer.fromFunction(Initializer.apply _)

  trait ChainIntegrity
  case class HalfEmptyInconsistency(error: String) extends ChainIntegrity
  case class GraphInconsistency(error: String) extends ChainIntegrity
  case object ChainEmpty extends ChainIntegrity
  case object ChainValid extends ChainIntegrity

}
