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
      Future.successful(ChainValid)
    }
  }

  def initFromDbAndDisk(implicit s: ActorSystem[Nothing], ref: ActorRef[ChainStateHolderRequest]): Future[ChainIntegrity] =
    backend.loadBlockInfoByEpochIndex
      .flatMap {
        case blockInfoByEpochIndex if blockInfoByEpochIndex.isEmpty =>
          Future {
            snapshotManager.clearAllSnapshots()
            require(backend.initGraph, "Janus graph must be empty when main db is empty, drop janusgraph keyspace!")
            logger.info(s"Chain is empty, loading from scratch ...")
            ChainState.empty
          }
        case blockInfoByEpochIndex =>
          val graphEmpty = backend.initGraph
          val txBoxesByEpochSource =
            if (graphEmpty) {
              logger.info(s"Graph is empty, loading from database")
              Source
                .fromIterator(() => blockInfoByEpochIndex.keysIterator)
                .flatMapConcat { epochIndex =>
                  Source(Epoch.heightRangeForEpochIndex(epochIndex))
                    .via(backend.transactionBoxesByHeightFlow)
                    .grouped(Const.EpochLength)
                    .map { boxesByHeight =>
                      boxesByHeight.iterator
                        .foreach { case (height, boxesByTx) =>
                          boxesByTx.foreach { case (tx, (inputs, outputs)) =>
                            TxGraphWriter.writeGraph(tx, height, inputs, outputs)(backend.janusGraph)
                          }
                        }
                      backend.janusGraph.tx().commit()
                      logger.info(s"Graph building of epoch $epochIndex finished")
                      boxesByHeight
                    }
                    .async(ActorAttributes.IODispatcher.dispatcher)
                }
            } else {
              Source
                .fromIterator(() => blockInfoByEpochIndex.keysIterator)
                .mapConcat(Epoch.heightRangeForEpochIndex)
                .via(backend.transactionBoxesByHeightFlow)
                .grouped(Const.EpochLength)
            }
          val utxoStateF =
            if (snapshotManager.latestSerializedSnapshot.exists(_.epochIndex == blockInfoByEpochIndex.lastKey)) {
              if (graphEmpty) {
                txBoxesByEpochSource.run().flatMap { _ =>
                  snapshotManager.getLatestSnapshotByIndex.map(_.get.utxoState)
                }
              } else {
                snapshotManager.getLatestSnapshotByIndex.map(_.get.utxoState)
              }
            } else {
              logger.info(s"Loading utxoState from database ... ")
              txBoxesByEpochSource
                .runFold(UtxoState.empty) { case (s, boxesByHeight) =>
                  val epochIndex = Epoch.epochIndexForHeight(boxesByHeight.head._1)
                  logger.info(s"Merging boxes of epoch $epochIndex finished")
                  s.mergeGivenBoxes(boxesByHeight.iterator.flatMap(_._2.iterator.map(_._2)))
                }
            }
          utxoStateF.map(utxoState => ChainState(blockInfoByEpochIndex, utxoState))
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

  case class MissingEpochs(missingEpochIndexes: TreeSet[EpochIndex]) extends ChainIntegrity {
    def missingHeights: TreeSet[EpochIndex] = missingEpochIndexes.flatMap(Epoch.heightRangeForEpochIndex)
  }

}
