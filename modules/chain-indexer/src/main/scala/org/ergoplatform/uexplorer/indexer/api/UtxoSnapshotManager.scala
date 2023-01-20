package org.ergoplatform.uexplorer.indexer.api

import org.ergoplatform.uexplorer.indexer.chain.Epoch
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState

import java.io.File
import scala.concurrent.Future

trait UtxoSnapshotManager {
  def clearAllSnapshots(): Unit
  def latestSerializedSnapshot: Option[UtxoSnapshot.Serialized]
  def makeSnapshotOnEpoch(newEpochOpt: Option[Epoch], utxoState: UtxoState): Future[Unit]
  def saveSnapshot(snapshot: UtxoSnapshot.Deserialized, force: Boolean): Future[Unit]
  def getLatestSnapshotByIndex: Future[Option[UtxoSnapshot.Deserialized]]
}

object UtxoSnapshot {
  case class Serialized(epochIndex: Int, utxoStateDir: File)
  case class Deserialized(epochIndex: Int, utxoState: UtxoState)
}
