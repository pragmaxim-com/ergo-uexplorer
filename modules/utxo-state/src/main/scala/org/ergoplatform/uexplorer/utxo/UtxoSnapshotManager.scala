package org.ergoplatform.uexplorer.utxo

import org.ergoplatform.uexplorer.Epoch
import org.ergoplatform.uexplorer.utxo.UtxoState

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
