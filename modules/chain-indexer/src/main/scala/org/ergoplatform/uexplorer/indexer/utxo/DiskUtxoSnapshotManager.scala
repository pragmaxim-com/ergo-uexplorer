package org.ergoplatform.uexplorer.indexer.utxo

import akka.actor.typed.ActorSystem
import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, Framing, Source}
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.api.{UtxoSnapshot, UtxoSnapshotManager}
import org.ergoplatform.uexplorer.indexer.chain.ChainStateHolder.NewEpochDetected
import org.ergoplatform.uexplorer.indexer.chain.Epoch
import org.ergoplatform.uexplorer.{Address, BoxId}

import java.io.*
import java.nio.file.{Files, Path, Paths}
import java.util.Comparator
import scala.collection.immutable.TreeMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.Try
import scala.util.control.NonFatal

class DiskUtxoSnapshotManager(
  rootSnapshotDir: File = Paths.get(System.getProperty("user.home"), ".ergo-uexplorer", "snapshots").toFile
)(implicit s: ActorSystem[Nothing])
  extends UtxoSnapshotManager
  with LazyLogging {

  def clearAllSnapshots(): Unit =
    if (rootSnapshotDir.exists()) {
      logger.info(s"Deleting all legacy snapshots")
      Files.walk(rootSnapshotDir.toPath).sorted(Comparator.reverseOrder[Path]).iterator.asScala.map(_.toFile.delete())
    }

  def latestSerializedSnapshot: Option[UtxoSnapshot.Serialized] =
    if (rootSnapshotDir.exists()) {
      val snapshots = rootSnapshotDir.listFiles().collect {
        case file if file.getName.toIntOption.nonEmpty => UtxoSnapshot.Serialized(file.getName.toInt, file)
      }
      snapshots.sortBy(_.epochIndex).lastOption
    } else None

  def makeSnapshotOnEpoch(newEpochOpt: Option[Epoch], utxoState: UtxoState): Future[Unit] =
    newEpochOpt.fold(Future.successful(())) { newEpoch =>
      saveSnapshot(UtxoSnapshot.Deserialized(newEpoch.index, utxoState))
    }

  def saveSnapshot(snapshot: UtxoSnapshot.Deserialized): Future[Unit] =
    Future(rootSnapshotDir.mkdirs()).flatMap { _ =>
      val snapshotDir = rootSnapshotDir.toPath.resolve(snapshot.epochIndex.toString).toFile
      logger.info(s"Saving snapshot at epoch ${snapshot.epochIndex} to ${snapshotDir.getPath}")
      if (snapshotDir.exists()) {
        Option(snapshotDir.listFiles()).foreach(_.foreach(_.delete()))
      }
      snapshotDir.mkdirs()
      Source
        .fromIterator(() => snapshot.utxoState.addressByUtxo.iterator)
        .map { case (boxId, address) => ByteString(s"$boxId $address\n") }
        .runWith(FileIO.toPath(f = snapshotDir.toPath.resolve("addressByUtxo")))
        .flatMap { _ =>
          Source
            .fromIterator(() => snapshot.utxoState.utxosByAddress.iterator)
            .map { case (address, utxos) =>
              ByteString(s"$address ${utxos.map { case (b, v) => s"$b:$v" }.mkString(",")}\n")
            }
            .runWith(FileIO.toPath(f = snapshotDir.toPath.resolve("utxosByAddress")))
            .map(_ => ())
        }
    }

  def getLatestSnapshotByIndex: Future[Option[UtxoSnapshot.Deserialized]] =
    latestSerializedSnapshot
      .map { case UtxoSnapshot.Serialized(latestEpochIndex, snapshotDir) =>
        logger.info(s"Loading snapshot at epoch $latestEpochIndex from ${snapshotDir.getPath}")
        FileIO
          .fromPath(f = snapshotDir.toPath.resolve("addressByUtxo"))
          .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = Int.MaxValue))
          .map(line => line.utf8String.split(' '))
          .map(arr => BoxId(arr(0)) -> Address.fromStringUnsafe(arr(1)))
          .runFold(Map.newBuilder[BoxId, Address]) { case (acc, tuple) =>
            acc.addOne(tuple)
          }
          .map(_.result())
          .flatMap { addressByUtxo =>
            FileIO
              .fromPath(f = snapshotDir.toPath.resolve("utxosByAddress"))
              .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = Int.MaxValue))
              .map(line => line.utf8String.split(' '))
              .map(arr =>
                Address.fromStringUnsafe(arr(0)) -> arr(1)
                  .split(",")
                  .map(_.split(':'))
                  .map(arr => BoxId(arr(0)) -> arr(1).toLong)
                  .toMap
              )
              .runFold(Map.newBuilder[Address, Map[BoxId, Long]]) { case (acc, tuple) =>
                acc.addOne(tuple)
              }
              .map(_.result())
              .map(utxosByAddress =>
                Option(UtxoSnapshot.Deserialized(latestEpochIndex, UtxoState(addressByUtxo, utxosByAddress)))
              )
          }
      }
      .getOrElse(Future.successful(None))
}
