package org.ergoplatform.uexplorer.indexer.db

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
import org.apache.tinkerpop.gremlin.structure.{Graph, Transaction}
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.db.{BestBlockInserted, FullBlock}
import pureconfig.ConfigReader
import zio.*

import java.util.concurrent.ConcurrentHashMap
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.{ArraySeq, TreeMap}
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.{Success, Try}
import org.ergoplatform.uexplorer.db.GraphBackend
import org.ergoplatform.uexplorer.indexer.config.{ChainIndexerConf, InMemoryGraph, JanusGraph}

object GraphBackend {
  def layer: ZLayer[ChainIndexerConf, Throwable, GraphBackend] =
    ZLayer.service[ChainIndexerConf].flatMap { confEnv =>
      confEnv.get.graphBackendType match {
        case JanusGraph(parallelism) =>
          ZLayer.scoped(
            ZIO.acquireRelease(
              ZIO.attempt(new InMemoryGraphBackend) // TODO switch to JanusGraph
            )(gb => ZIO.succeed(gb.close()))
          )
        case InMemoryGraph(parallelism) =>
          ZLayer.scoped(
            ZIO.acquireRelease(
              ZIO.attempt(new InMemoryGraphBackend)
            )(gb => ZIO.succeed(gb.close()))
          )
      }
    }
}

class InMemoryGraphBackend extends GraphBackend {

  private var initialized = true

  def initGraph: Boolean =
    if (initialized) {
      initialized = false
      true
    } else {
      false
    }

  def writeTxsAndCommit(block: BestBlockInserted): Task[BestBlockInserted] = ZIO.succeed(block)

  def isEmpty: Boolean = true

  def graphTraversalSource: GraphTraversalSource = EmptyGraph.instance.traversal()

  def close(): Task[Unit] = ZIO.succeed(())

}
