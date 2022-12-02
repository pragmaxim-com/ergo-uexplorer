package org.ergoplatform.uexplorer.indexer.chain

import akka.{Done, NotUsed}
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.pattern.StatusReply
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.{Address, BoxId}
import org.ergoplatform.uexplorer.indexer.chain.Epoch
import org.ergoplatform.uexplorer.indexer.*

import java.io.{File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.{Path, Paths}
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Success, Try}

case class UtxoState(
  addressByUtxo: Map[BoxId, Address],
  utxosByAddress: Map[Address, Map[BoxId, Long]],
  inputsWithoutAddress: Set[BoxId]
) {

  def mergeEpochFromBuffer(
    boxesByHeight: Iterator[(Int, (ArraySeq[BoxId], ArraySeq[(BoxId, Address, Long)]))]
  ): UtxoState = {
    val (inputsBuilder, newAddressByUtxo, boxIdsByAddressWithOutputs) =
      boxesByHeight
        .foldLeft((ArraySeq.newBuilder[BoxId], addressByUtxo, utxosByAddress)) {
          case ((inputBoxIdsAcc, addressByUtxoAcc, utxosByAddressAcc), (_, (inputBoxIds, outputBoxIdsWithAddress))) =>
            (
              inputBoxIdsAcc.addAll(inputBoxIds),
              addressByUtxoAcc ++ outputBoxIdsWithAddress.iterator.map(o => o._1 -> o._2),
              outputBoxIdsWithAddress
                .foldLeft(utxosByAddressAcc) { case (acc, (boxId, address, value)) =>
                  acc.adjust(address)(_.fold(Map(boxId -> value))(_.updated(boxId, value)))
                }
            )
        }

    val inputs = inputsBuilder.result()
    val (inputsWithAddress, inputsWoAddress) =
      inputs.foldLeft(mutable.Map.empty[Address, mutable.Set[BoxId]] -> ArraySeq.newBuilder[BoxId]) {
        case ((inputsWithAddressAcc, inputsWoAddressAcc), boxId) =>
          if (newAddressByUtxo.contains(boxId)) {
            val address = newAddressByUtxo(boxId)
            inputsWithAddressAcc.adjust(address)(
              _.fold(mutable.Set(boxId))(_.addOne(boxId))
            ) -> inputsWoAddressAcc
          } else
            inputsWithAddressAcc -> inputsWoAddressAcc.addOne(boxId)
      }
    val utxosByAddressWoInputs =
      inputsWithAddress
        .foldLeft(boxIdsByAddressWithOutputs) { case (acc, (address, inputIds)) =>
          acc.putOrRemove(address) {
            case None                 => None
            case Some(existingBoxIds) => Option(existingBoxIds.removedAll(inputIds)).filter(_.nonEmpty)
          }
        }
    UtxoState(
      newAddressByUtxo -- inputs,
      utxosByAddressWoInputs,
      inputsWithoutAddress ++ inputsWoAddress.result()
    )
  }
}

object UtxoState extends LazyLogging {
  def empty: UtxoState = UtxoState(Map.empty, Map.empty, Set.empty)

  private lazy val snapshotDir = Paths.get(System.getProperty("user.home"), ".ergo-uexplorer", "snapshots").toFile

  def latestSnapshot: Option[(Int, File)] =
    if (snapshotDir.exists()) {
      val snapshots = snapshotDir.listFiles().collect {
        case file if file.getName.toIntOption.nonEmpty => file.getName.toInt -> file
      }
      TreeMap(snapshots: _*).lastOption
    } else None

  def saveSnapshot(latestEpochIndex: Int, utxoState: UtxoState): Try[Unit] =
    Try(snapshotDir.mkdirs()).map { _ =>
      val snapshotFile = snapshotDir.toPath.resolve(latestEpochIndex.toString).toFile
      logger.info(s"Saving snapshot at epoch $latestEpochIndex to ${snapshotFile.getPath}")
      snapshotFile.delete()
      snapshotFile.createNewFile()
      val fileOutStream   = new FileOutputStream(snapshotFile)
      val objectOutStream = new ObjectOutputStream(fileOutStream)
      try objectOutStream.writeObject(utxoState)
      catch {
        case NonFatal(ex) =>
          logger.error(s"Unable to save snapshot ${snapshotFile.getPath}", ex)
          throw ex
      } finally {
        objectOutStream.close()
        fileOutStream.close()
      }
    }

  def getLatestSnapshotByIndex: Try[(Int, UtxoState)] =
    Try(latestSnapshot.get).map { case (latestEpochIndex, snapshotFile) =>
      logger.info(s"Loading snapshot at epoch $latestEpochIndex from ${snapshotFile.getPath}")
      val fileInput   = new FileInputStream(snapshotFile)
      val objectInput = new ObjectInputStream(fileInput)
      try latestEpochIndex -> objectInput.readObject.asInstanceOf[UtxoState]
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
