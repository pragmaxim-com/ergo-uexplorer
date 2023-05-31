package org.ergoplatform.uexplorer.indexer.chain

import akka.actor.typed.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.gremlin.structure.Graph
import org.ergoplatform.uexplorer.{Const, Epoch, Height}
import org.ergoplatform.uexplorer.cassandra.api.Backend
import org.ergoplatform.uexplorer.janusgraph.api.GraphBackend
import org.ergoplatform.uexplorer.utxo.MvUtxoState
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.collection.immutable.TreeSet
import org.ergoplatform.uexplorer.indexer.chain.Initializer.*

class Initializer(
  utxoState: MvUtxoState,
  backend: Backend,
  graphBackend: GraphBackend
) extends LazyLogging {

  def init(implicit s: ActorSystem[Nothing]): Future[ChainIntegrity] =
    if (!backend.isEmpty && utxoState.isEmpty) {
      logger.info("Initializing utxoState from backend")
      utxoState
        .load(backend.getAllBlockIdsAndHeight)
        .flatMap { _ =>
          utxoState.init(backend.getBlockInfo).flatMap { _ =>
            val blocksByHeight = utxoState.getBlocksByHeight
            val includingGraph = graphBackend.initGraph
            Source
              .fromIterator(() => blocksByHeight)
              .via(backend.transactionBoxesByHeightFlow)
              .buffer(Const.EpochLength, OverflowStrategy.backpressure)
              .runFold[(ChainIntegrity, Graph)](ChainValid(0) -> graphBackend.tx.createThreadedTx[Graph]()) {
                case ((MissingBlocks(latestHeight, missingHeights), threadedGraph), ((height, blockId), boxesByTx)) =>
                  if (height != latestHeight + 1) {
                    logger.error(s"Chain integrity is broken at height $height for blockId $blockId")
                    MissingBlocks(height, missingHeights ++ ((latestHeight + 1) until height)) -> threadedGraph
                  } else {
                    MissingBlocks(height, missingHeights) -> threadedGraph
                  }
                case ((ChainValid(latestHeight), threadedGraph), ((height, blockId), boxesByTx)) =>
                  if (height != latestHeight + 1) {
                    logger.error(s"Chain integrity is broken at height $height for blockId $blockId")
                    MissingBlocks(height, TreeSet((latestHeight + 1 until height): _*)) -> threadedGraph
                  } else {
                    utxoState.mergeBlockBoxesUnsafe(height, blockId, boxesByTx.iterator.map(_._2))
                    val newG =
                      if (includingGraph) {
                        graphBackend.writeTx(height, boxesByTx, utxoState.getAddressStats, threadedGraph)
                        if (height % Const.EpochLength == 0) {
                          threadedGraph.tx.commit()
                          logger.info(s"Graph building of epoch ${Epoch.epochIndexForHeight(height)} finished")
                          graphBackend.tx.createThreadedTx[Graph]()
                        } else threadedGraph
                      } else threadedGraph
                    ChainValid(height) -> newG
                  }
              }
              .map { case (chainIntegrity, graph) =>
                graph.tx.close()
                chainIntegrity
              }
          }
        }
    } else if (backend.isEmpty && !utxoState.isEmpty) {
      Future.successful(GraphInconsistency(s"utxoState must be empty when backend is."))
    } else if (backend.isEmpty && utxoState.isEmpty) {
      if (graphBackend.initGraph || graphBackend.isEmpty) {
        logger.info(s"Chain is empty, loading from scratch ...")
        Future.successful(ChainEmpty)
      } else {
        Future.successful(GraphInconsistency("Janus graph must be empty when main db is empty, drop janusgraph keyspace!"))
      }
    } else {
      utxoState.init(backend.getBlockInfo).map(_ => ChainValid(utxoState.getLastBlock.map(_._1).get))
    }

}

object Initializer {
  trait ChainIntegrity

  case class GraphInconsistency(error: String) extends ChainIntegrity
  case object ChainEmpty extends ChainIntegrity
  case class ChainValid(latestHeight: Height) extends ChainIntegrity
  case class MissingBlocks(latestHeight: Height, missingHeights: TreeSet[Height]) extends ChainIntegrity

}
