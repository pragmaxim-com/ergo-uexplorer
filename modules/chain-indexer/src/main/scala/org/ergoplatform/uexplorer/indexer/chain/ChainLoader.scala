package org.ergoplatform.uexplorer.indexer.chain

import akka.Done
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.pattern.StatusReply
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.UnexpectedStateError
import org.ergoplatform.uexplorer.indexer.api.{Backend, UtxoSnapshotManager}
import org.ergoplatform.uexplorer.indexer.chain.ChainState.*
import org.ergoplatform.uexplorer.indexer.chain.ChainStateHolder.ChainStateHolderRequest
import org.ergoplatform.uexplorer.indexer.config.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId, Const}

import scala.collection.immutable.{TreeMap, TreeSet}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

class ChainLoader(
  backend: Backend,
  snapshotManager: UtxoSnapshotManager
) extends LazyLogging {

  import ChainLoader.*

  private def verifyStateIntegrity(
    chainState: ChainState,
    snapshotManager: UtxoSnapshotManager
  ): Future[ChainIntegrity] = {
    val missingEpochIndexes = chainState.findMissingEpochIndexes
    if (missingEpochIndexes.nonEmpty) {
      logger.error(s"Going to index missing blocks for epochs : ${missingEpochIndexes.mkString(", ")}")
      Future(snapshotManager.clearAllSnapshots()).map(_ => MissingEpochs(missingEpochIndexes))
    } else if (chainState.utxoState.utxosByAddress.contains(Const.FeeContract.address)) {
      Future(snapshotManager.clearAllSnapshots()).flatMap { _ =>
        Future.failed(
          new UnexpectedStateError("UtxoState should not contain Fee Contract address as all such boxes should be spent")
        )
      }
    } else {
      logger.info(s"Chain state is valid")
      Future.successful(ChainValid)
    }
  }

  def initFromDbAndDisk(implicit s: ActorSystem[Nothing], ref: ActorRef[ChainStateHolderRequest]): Future[ChainIntegrity] =
    backend.loadBlockInfoByEpochIndex
      .flatMap {
        case blockInfoByEpochIndex if blockInfoByEpochIndex.isEmpty =>
          Future {
            snapshotManager.clearAllSnapshots()
            ChainState.empty
          }
        case blockInfoByEpochIndex
            if snapshotManager.latestSerializedSnapshot.exists(_.epochIndex == blockInfoByEpochIndex.lastKey) =>
          snapshotManager.getLatestSnapshotByIndex
            .map { snapshotOpt =>
              ChainState(blockInfoByEpochIndex, snapshotOpt.get.utxoState)
            }
        case blockInfoByEpochIndex =>
          backend.loadUtxoState(blockInfoByEpochIndex.keysIterator).map { utxoState =>
            snapshotManager.clearAllSnapshots()
            ChainState(blockInfoByEpochIndex, utxoState)
          }
      }
      .flatMap { chainState =>
        ChainStateHolder.initialize(chainState).flatMap { _ =>
          verifyStateIntegrity(chainState, snapshotManager)
        }
      }

}

object ChainLoader {
  trait ChainIntegrity

  case object ChainValid extends ChainIntegrity

  case class MissingEpochs(missingEpochIndexes: TreeSet[Int]) extends ChainIntegrity {
    def missingHeights: TreeSet[Int] = missingEpochIndexes.flatMap(Epoch.heightRangeForEpochIndex)
  }

}
