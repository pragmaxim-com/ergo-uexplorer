package org.ergoplatform.uexplorer.indexer.chain

import akka.Done
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.pattern.StatusReply
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.api.{Backend, GraphBackend, UtxoSnapshot, UtxoSnapshotManager}
import org.ergoplatform.uexplorer.indexer.chain.ChainState.*
import org.ergoplatform.uexplorer.indexer.chain.ChainStateHolder.ChainStateHolderRequest
import org.ergoplatform.uexplorer.indexer.janusgraph.TxGraphWriter
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId, Const, EpochIndex}

import scala.collection.immutable.{TreeMap, TreeSet}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}
import org.ergoplatform.uexplorer.AkkaStreamSupport

class ChainLoader(
  backend: Backend,
  graphBackend: GraphBackend,
  snapshotManager: UtxoSnapshotManager
)(implicit s: ActorSystem[Nothing])
  extends AkkaStreamSupport
  with LazyLogging {

  import ChainLoader.*

  private def loadUtxoStateFromDb(blockInfoByEpochIndex: TreeMap[Int, BufferedBlockInfo], includingGraph: Boolean) = {
    val subjectToLoad = if (includingGraph) "graph and utxoState" else "utxoState"
    logger.info(s"Loading $subjectToLoad from database ... ")
    Source
      .fromIterator(() => blockInfoByEpochIndex.keysIterator)
      .mapConcat(Epoch.heightRangeForEpochIndex)
      .via(backend.transactionBoxesByHeightFlow)
      .grouped(Const.EpochLength)
      .runFoldAsync(UtxoState.empty) { case (s, boxesByHeight) =>
        Future {
          val epochIndex = Epoch.epochIndexForHeight(boxesByHeight.head._1)
          logger.info(s"Merging boxes of epoch $epochIndex finished")
          val newState = s.mergeGivenBoxes(boxesByHeight.last._1, boxesByHeight.iterator.flatMap(_._2.iterator))
          if (includingGraph) {
            graphBackend.writeTxsAndCommit(boxesByHeight, newState.topAddresses.nodeMap)
            logger.info(s"Graph building of epoch $epochIndex finished")
          }
          newState
        }
      }
  }

  private def initUtxoState(blockInfoByEpochIndex: TreeMap[EpochIndex, BufferedBlockInfo]) =
    if (blockInfoByEpochIndex.isEmpty)
      Future {
        snapshotManager.clearAllSnapshots()
        require(
          graphBackend.initGraph || graphBackend.isEmpty,
          "Janus graph must be empty when main db is empty, drop janusgraph keyspace!"
        )
        logger.info(s"Chain is empty, loading from scratch ...")
        UtxoState.empty
      }
    else {
      val graphEmpty = graphBackend.initGraph
      lazy val snapshotExists =
        snapshotManager.latestSerializedSnapshot.exists(_.epochIndex == blockInfoByEpochIndex.lastKey)
      if (!graphEmpty && snapshotExists) {
        logger.info("Graph is already initialized and utxo snapshot exists, let's just load it from disk")
        snapshotManager.getLatestSnapshotByIndex.map(_.get.utxoState)
      } else {
        loadUtxoStateFromDb(blockInfoByEpochIndex, graphEmpty)
      }
    }

  def initChainStateFromDbAndDisk: Future[ChainState] =
    for {
      blockInfoByEpochIndex <- backend.loadBlockInfoByEpochIndex
      utxoState             <- initUtxoState(blockInfoByEpochIndex)
    } yield ChainState(blockInfoByEpochIndex, utxoState)

  def verifyStateIntegrity(chainState: ChainState): Future[ChainIntegrity] = {
    val missingEpochIndexes = chainState.findMissingEpochIndexes
    if (missingEpochIndexes.nonEmpty) {
      logger.error(s"Going to index missing blocks for epochs : ${missingEpochIndexes.mkString(", ")}")
      Future(snapshotManager.clearAllSnapshots()).map(_ => MissingEpochs(missingEpochIndexes))
    } else {
      logger.info(s"Chain state is valid, making utxo state snapshot if it does not exists")
      snapshotManager
        .saveSnapshot(UtxoSnapshot.Deserialized(chainState.persistedEpochIndexes.last, chainState.utxoState), force = false)
        .map(_ => ChainValid(chainState))
    }
  }

}

object ChainLoader {
  trait ChainIntegrity

  case class ChainValid(chainState: ChainState) extends ChainIntegrity

  case class MissingEpochs(missingEpochIndexes: TreeSet[EpochIndex]) extends ChainIntegrity {

    def missingHeights: TreeSet[EpochIndex] =
      missingEpochIndexes.flatMap(idx => List(idx, idx + 1)).flatMap(Epoch.heightRangeForEpochIndex)
  }

}
