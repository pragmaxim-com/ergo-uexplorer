package org.ergoplatform.uexplorer.indexer.db

import akka.actor.typed.ActorSystem
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.backend.H2Backend
import org.ergoplatform.uexplorer.cassandra.CassandraBackend
import org.ergoplatform.uexplorer.db.Backend
import pureconfig.ConfigReader

import java.util.concurrent.ConcurrentHashMap
import scala.collection.compat.immutable.ArraySeq
import scala.jdk.CollectionConverters.*
import scala.util.Try

object Backend {
  import pureconfig.generic.derivation.default.*

  sealed trait BackendType derives ConfigReader

  case class Cassandra(parallelism: Int) extends BackendType

  case object H2 extends BackendType

  def apply(backendType: BackendType)(implicit system: ActorSystem[Nothing]): Try[Backend] = backendType match {
    case Cassandra(parallelism) =>
      CassandraBackend(parallelism)
    case H2 =>
      H2Backend(system)
  }

}
