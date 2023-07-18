package org.ergoplatform.uexplorer.plugin

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.db.{BestBlockInserted, FullBlock}
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.{BoxId, ErgoTreeHex, ReadableStorage, TxId, Value}
import zio.*
import scala.collection.immutable.ListMap
import scala.util.Try

trait Plugin {

  def name: String

  def init: Task[Unit]

  def close: Task[Unit]

  def processMempoolTx(
    newTx: ApiTransaction,
    storage: ReadableStorage,
    graphTraversalSource: GraphTraversalSource
  ): Task[Unit]

  def processNewBlock(
    newBlock: BestBlockInserted,
    storage: ReadableStorage,
    graphTraversalSource: GraphTraversalSource
  ): Task[Unit]
}
