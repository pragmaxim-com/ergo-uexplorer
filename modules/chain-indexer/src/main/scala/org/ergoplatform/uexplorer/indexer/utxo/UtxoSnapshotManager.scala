package org.ergoplatform.uexplorer.indexer.utxo

import com.typesafe.scalalogging.LazyLogging

import java.io.{File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.Paths
import scala.collection.immutable.TreeMap
import scala.util.Try
import scala.util.control.NonFatal

object UtxoSnapshot {
  case class Serialized(epochIndex: Int, file: File)
  case class Deserialized(epochIndex: Int, utxoState: UtxoState)
}

class UtxoSnapshotManager(
  snapshotDir: File = Paths.get(System.getProperty("user.home"), ".ergo-uexplorer", "snapshots").toFile
) extends LazyLogging {

  def clearAllSnapshots: Unit =
    if (snapshotDir.exists()) {
      snapshotDir.listFiles().foreach(_.delete())
    }

  def latestSerializedSnapshot: Option[UtxoSnapshot.Serialized] =
    if (snapshotDir.exists()) {
      val snapshots = snapshotDir.listFiles().collect {
        case file if file.getName.toIntOption.nonEmpty => UtxoSnapshot.Serialized(file.getName.toInt, file)
      }
      snapshots.sortBy(_.epochIndex).lastOption
    } else None

  def saveSnapshot(snapshot: UtxoSnapshot.Deserialized): Try[Unit] =
    Try(snapshotDir.mkdirs()).map { _ =>
      val snapshotFile = snapshotDir.toPath.resolve(snapshot.epochIndex.toString).toFile
      logger.info(s"Saving snapshot at epoch ${snapshot.epochIndex} to ${snapshotFile.getPath}")
      snapshotFile.delete()
      snapshotFile.createNewFile()
      val fileOutStream   = new FileOutputStream(snapshotFile)
      val objectOutStream = new ObjectOutputStream(fileOutStream)
      try objectOutStream.writeObject(snapshot.utxoState)
      catch {
        case NonFatal(ex) =>
          logger.error(s"Unable to save snapshot ${snapshotFile.getPath}", ex)
          throw ex
      } finally {
        objectOutStream.close()
        fileOutStream.close()
      }
    }

  def getLatestSnapshotByIndex: Try[UtxoSnapshot.Deserialized] =
    Try(latestSerializedSnapshot.get).map { case UtxoSnapshot.Serialized(latestEpochIndex, snapshotFile) =>
      logger.info(s"Loading snapshot at epoch $latestEpochIndex from ${snapshotFile.getPath}")
      val fileInput   = new FileInputStream(snapshotFile)
      val objectInput = new ObjectInputStream(fileInput)
      try UtxoSnapshot.Deserialized(latestEpochIndex, objectInput.readObject.asInstanceOf[UtxoState])
      catch {
        case NonFatal(ex) =>
          logger.error(s"Unable to load snapshot ${snapshotFile.getPath}", ex)
          throw ex
      } finally {
        objectInput.close()
        fileInput.close()
      }
    }

}
