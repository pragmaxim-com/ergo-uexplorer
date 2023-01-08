package org.ergoplatform.uexplorer.plugin

import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.{Address, BoxId, TxId, Value}
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateWithPool, UtxoStateWithoutPool}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource

import scala.collection.immutable.ListMap
import scala.concurrent.Future
import scala.util.Try

trait Plugin {

  def name: String

  def init: Future[Unit]

  def close: Future[Unit]

  def processMempoolTx(
    newTx: ApiTransaction,
    utxoStateWoPool: UtxoStateWithoutPool,
    utxoStateWithPool: UtxoStateWithPool,
    graphTraversalSource: GraphTraversalSource
  ): Future[Unit]

  def processNewBlock(
    newBlock: Block,
    utxoStateWoPool: UtxoStateWithoutPool,
    graphTraversalSource: GraphTraversalSource
  ): Future[Unit]
}

object Plugin {

  sealed trait UtxoStateLike {
    def addressByUtxo: Map[BoxId, Address]
    def utxosByAddress: Map[Address, Map[BoxId, Value]]
  }

  case class UtxoStateWithoutPool(
    addressByUtxo: Map[BoxId, Address],
    utxosByAddress: Map[Address, Map[BoxId, Value]]
  ) extends UtxoStateLike

  case class UtxoStateWithPool(
    addressByUtxo: Map[BoxId, Address],
    utxosByAddress: Map[Address, Map[BoxId, Value]]
  ) extends UtxoStateLike

}
