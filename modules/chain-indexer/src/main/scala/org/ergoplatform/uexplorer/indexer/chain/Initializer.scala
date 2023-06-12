package org.ergoplatform.uexplorer.indexer.chain

import akka.actor.typed.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.gremlin.structure.Graph
import org.ergoplatform.uexplorer.{BlockId, BoxesByTx, Const, Epoch, Height}
import org.ergoplatform.uexplorer.cassandra.api.Backend
import org.ergoplatform.uexplorer.janusgraph.api.GraphBackend
import org.ergoplatform.uexplorer.utxo.MvUtxoState

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.collection.immutable.TreeSet
import org.ergoplatform.uexplorer.indexer.chain.Initializer.*

import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.util.Try

class Initializer(
  utxoState: MvUtxoState,
  backend: Backend,
  graphBackend: GraphBackend
) extends LazyLogging {

  private def mergeTxToUtxoState(height: Height, blockId: BlockId, boxesByTx: BoxesByTx): Unit = {
    utxoState.mergeBlockBoxesUnsafe(height, blockId, boxesByTx.iterator.map(_._2))
    if (height % 1000 == 0) {
      logger.info(
        s"Height $height, utxo count: ${utxoState.utxoBoxCount}, non-empty-address count: ${utxoState.nonEmptyAddressCount}"
      )
    }
    if (height % 100000 == 0) {
      logger.info("Compacting mvstore")
      utxoState.compactFile(60000 * 10) // 10 minutes
      if (Files.size(Paths.get("/home/lisak/.ergo-uexplorer/utxo/mvstore")) > 128849018880L) {
        logger.error(s"File storage size overlfow")
        System.exit(1)
      }
    }
  }

  private def mergeTxToGraph(height: Height, boxesByTx: BoxesByTx, includingGraph: Boolean, g: Graph): Future[Graph] =
    if (includingGraph) {
      Future {
        graphBackend.writeTx(height, boxesByTx, utxoState.getAddressStats, g)
        if (height % 50 == 0) {
          g.tx.commit()
          logger.info(s"Graph building of epoch ${Epoch.epochIndexForHeight(height)} finished")
          graphBackend.tx.createThreadedTx[Graph]()
        } else g
      }
    } else Future.successful(g)

  def init(implicit s: ActorSystem[Nothing]): Future[ChainIntegrity] =
    if (!backend.isEmpty && utxoState.isEmpty) {
      logger.info("Initializing utxoState from backend")
      backend.getAllBlockIdsAndHeight
        .runFold(mutable.TreeMap.newBuilder[Height, BlockId]) { case (acc, el) =>
          acc.addOne(el)
        }
        .flatMap { blockIdByHeightBuilder =>
          val blockIdByHeight = blockIdByHeightBuilder.result().dropRight(MvUtxoState.VersionsToKeep)
          logger.info(s"Loading ${blockIdByHeight.size} from backand into UtxoState")
          val includingGraph = graphBackend.initGraph
          if (includingGraph || graphBackend.isEmpty) {
            Source
              .fromIterator(() => blockIdByHeight.iterator)
              .via(backend.transactionBoxesByHeightFlow)
              .buffer(Const.EpochLength, OverflowStrategy.backpressure)
              .runFoldAsync[(ChainIntegrity, Graph)](ChainValid(0) -> graphBackend.tx.createThreadedTx[Graph]()) {
                case ((MissingBlocks(latestHeight, missingHeights), threadedGraph), ((height, blockId), _)) =>
                  if (height != latestHeight + 1) {
                    logger.error(s"Chain integrity is broken at height $height for blockId $blockId")
                    Future.successful(
                      MissingBlocks(height, missingHeights ++ ((latestHeight + 1) until height)) -> threadedGraph
                    )
                  } else {
                    Future.successful(MissingBlocks(height, missingHeights) -> threadedGraph)
                  }
                case ((ChainValid(latestHeight), threadedGraph), ((height, blockId), boxesByTx)) =>
                  if (height != latestHeight + 1) {
                    logger.error(s"Chain integrity is broken at height $height for blockId $blockId")
                    Future.successful(MissingBlocks(height, TreeSet((latestHeight + 1 until height): _*)) -> threadedGraph)
                  } else {
                    val utxoMergeF = Future(mergeTxToUtxoState(height, blockId, boxesByTx))
                    val newGF      = mergeTxToGraph(height, boxesByTx, includingGraph, threadedGraph)
                    utxoMergeF.flatMap(_ => newGF).map(ChainValid(height) -> _)
                  }
              }
              .map { case (chainIntegrity, graph) =>
                Try(graph.tx.commit()).map(_ => graph.tx.close())
                chainIntegrity
              }
          } else {
            Future.successful(
              GraphInconsistency("Indexing with empty UtxoState cannot proceed with non-empty JanusGraph!")
            )
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
      utxoState.getLastBlock
        .map(_._1)
        .map(ChainValid.apply)
        .fold(Future.failed(new IllegalStateException("UtxoState should not be empty")))(Future.successful)
    }

}

object Initializer {
  trait ChainIntegrity

  case class GraphInconsistency(error: String) extends ChainIntegrity
  case object ChainEmpty extends ChainIntegrity
  case class ChainValid(latestHeight: Height) extends ChainIntegrity
  case class MissingBlocks(latestHeight: Height, missingHeights: TreeSet[Height]) extends ChainIntegrity

}
