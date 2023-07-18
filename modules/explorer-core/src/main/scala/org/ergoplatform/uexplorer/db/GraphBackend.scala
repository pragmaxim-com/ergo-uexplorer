package org.ergoplatform.uexplorer.db

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Transaction
import zio.*

trait GraphBackend {

  def initGraph: Boolean

  def writeTxsAndCommit(block: BestBlockInserted): Task[BestBlockInserted]

  def graphTraversalSource: GraphTraversalSource

  def isEmpty: Boolean

  def close(): Task[Unit]
}
