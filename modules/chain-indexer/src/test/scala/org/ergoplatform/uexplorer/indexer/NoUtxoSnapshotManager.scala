package org.ergoplatform.uexplorer.indexer

import org.ergoplatform.uexplorer.indexer.utxo.{UtxoSnapshot, UtxoSnapshotManager, UtxoState}
import org.ergoplatform.uexplorer.Epoch

import scala.concurrent.Future

class NoUtxoSnapshotManager extends UtxoSnapshotManager {
  override def clearAllSnapshots(): Unit = Future.successful(())

  override def latestSerializedSnapshot: Option[UtxoSnapshot.Serialized] = Option.empty

  override def makeSnapshotOnEpoch(newEpochOpt: Option[Epoch], utxoState: UtxoState): Future[Unit] = Future.successful(())

  override def saveSnapshot(snapshot: UtxoSnapshot.Deserialized, force: Boolean): Future[Unit] = Future.successful(())

  override def getLatestSnapshotByIndex: Future[Option[UtxoSnapshot.Deserialized]] = Future.successful(Option.empty)
}
