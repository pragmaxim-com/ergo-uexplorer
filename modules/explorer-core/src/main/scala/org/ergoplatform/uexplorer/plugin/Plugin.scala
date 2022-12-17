package org.ergoplatform.uexplorer.plugin

import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.{Address, BoxId, TxId}
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateWithPool, UtxoStateWithoutPool}

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
    utxoStateWithPool: UtxoStateWithPool
  ): Future[Unit]

  def processNewBlock(
    newBlock: Block,
    utxoStateWoPool: UtxoStateWithoutPool
  ): Future[Unit]
}

object Plugin {

  sealed trait UtxoStateLike {
    def addressByUtxo: Map[BoxId, Address]
    def utxosByAddress: Map[Address, Map[BoxId, Long]]
  }

  case class UtxoStateWithoutPool(
    addressByUtxo: Map[BoxId, Address],
    utxosByAddress: Map[Address, Map[BoxId, Long]]
  ) extends UtxoStateLike

  case class UtxoStateWithPool(
    addressByUtxo: Map[BoxId, Address],
    utxosByAddress: Map[Address, Map[BoxId, Long]]
  ) extends UtxoStateLike

}
