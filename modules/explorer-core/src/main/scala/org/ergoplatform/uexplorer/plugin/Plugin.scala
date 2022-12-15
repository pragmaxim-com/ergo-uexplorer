package org.ergoplatform.uexplorer.plugin

import org.ergoplatform.uexplorer.{Address, BoxId, TxId}
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateWithPool, UtxoStateWithoutPool}

import scala.collection.immutable.ListMap
import scala.concurrent.Future
import scala.util.Try

trait Plugin {

  def name: String

  def init: Try[Unit]

  def execute(
    newTx: ApiTransaction,
    utxoStateWoPool: UtxoStateWithoutPool,
    utxoStateWithPool: UtxoStateWithPool
  ): Future[Unit]
}

object Plugin {

  case class UtxoStateWithoutPool(
    addressByUtxo: Map[BoxId, Address],
    utxosByAddress: Map[Address, Map[BoxId, Long]]
  )

  case class UtxoStateWithPool(
    addressByUtxo: Map[BoxId, Address],
    utxosByAddress: Map[Address, Map[BoxId, Long]]
  )

}
