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
import org.ergoplatform.uexplorer.indexer.{AkkaStreamSupport, UnexpectedStateError}
import org.ergoplatform.uexplorer.indexer.api.{Backend, UtxoSnapshotManager}
import org.ergoplatform.uexplorer.indexer.chain.ChainState.*
import org.ergoplatform.uexplorer.indexer.chain.ChainStateHolder.ChainStateHolderRequest
import org.ergoplatform.uexplorer.indexer.config.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.janusgraph.TxGraphWriter
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId, Const, EpochIndex}

import scala.collection.immutable.{TreeMap, TreeSet}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

class ChainLoader(
  backend: Backend,
  snapshotManager: UtxoSnapshotManager
) extends AkkaStreamSupport
  with LazyLogging {

  import ChainLoader.*

  private def verifyStateIntegrity(
    chainState: ChainState,
    snapshotManager: UtxoSnapshotManager
  ): Future[ChainIntegrity] = {
    val missingEpochIndexes = chainState.findMissingEpochIndexes
    if (missingEpochIndexes.nonEmpty) {
      logger.error(s"Going to index missing blocks for epochs : ${missingEpochIndexes.mkString(", ")}")
      Future(snapshotManager.clearAllSnapshots()).map(_ => MissingEpochs(missingEpochIndexes))
    } else {
      logger.info(s"Chain state is valid")
      Future.successful(ChainValid(chainState))
    }
  }

  def initFromDbAndDisk(
    verify: Boolean = true
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[ChainStateHolderRequest]): Future[ChainIntegrity] =
    backend.loadBlockInfoByEpochIndex
      .flatMap {
        case blockInfoByEpochIndex if blockInfoByEpochIndex.isEmpty =>
          Future {
            snapshotManager.clearAllSnapshots()
            require(
              backend.initGraph || backend.graphTraversalSource.V().hasNext,
              "Janus graph must be empty when main db is empty, drop janusgraph keyspace!"
            )
            logger.info(s"Chain is empty, loading from scratch ...")
            ChainState.empty
          }
        case blockInfoByEpochIndex =>
          val graphEmpty = backend.initGraph
          lazy val snapshotExists =
            snapshotManager.latestSerializedSnapshot.exists(_.epochIndex == blockInfoByEpochIndex.lastKey)
          val utxoStateF =
            if (!graphEmpty && snapshotExists) {
              logger.info("Graph is already initialized and utxo snapshot exists, let's just load it from disk")
              snapshotManager.getLatestSnapshotByIndex.map(_.get.utxoState)
            } else {
              val subjectToLoad =
                if (graphEmpty) {
                  "graph and utxoState"
                } else
                  "utxoState"
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
                    val newState = s.mergeGivenBoxes(boxesByHeight.iterator.flatMap(_._2.iterator))
                    if (graphEmpty) {
                      boxesByHeight.iterator
                        .foreach { case (height, boxesByTx) =>
                          boxesByTx.foreach { case (tx, (inputs, outputs)) =>
                            TxGraphWriter.writeGraph(tx, height, inputs, outputs, newState.topAddresses.nodeMap)(
                              backend.janusGraph
                            )
                          }
                        }
                      backend.janusGraph.tx().commit()
                      logger.info(s"Graph building of epoch $epochIndex finished")
                    }
                    newState
                  }
                }
            }
          utxoStateF.map(utxoState => ChainState(blockInfoByEpochIndex, utxoState))
      }
      .flatMap { chainState =>
        ChainStateHolder.initialize(chainState).flatMap { _ =>
          if (verify)
            verifyStateIntegrity(chainState, snapshotManager)
          else
            Future.successful(ChainValid(chainState))
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
