package org.ergoplatform.uexplorer.indexer.chain

import org.ergoplatform.uexplorer.cassandra.api.Backend
import org.ergoplatform.uexplorer.janusgraph.api.GraphBackend
import org.ergoplatform.uexplorer.http.BlockHttpClient
import org.ergoplatform.uexplorer.utxo.MvUtxoState
import akka.actor.typed.ActorSystem
import akka.stream.SharedKillSwitch
import com.typesafe.scalalogging.LazyLogging
import akka.stream.scaladsl.Flow
import org.ergoplatform.uexplorer.db.Block
import akka.NotUsed
import akka.stream.scaladsl.Sink

import scala.concurrent.Future
import org.ergoplatform.uexplorer.Height
import akka.stream.OverflowStrategy
import akka.stream.ActorAttributes
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.indexer.chain.BlockIndexer.ChainSyncResult
import org.ergoplatform.uexplorer.ProtocolSettings

import scala.concurrent.ExecutionContext.Implicits.global
import org.ergoplatform.uexplorer.Resiliency
import org.ergoplatform.uexplorer.db.Inserted
import org.ergoplatform.uexplorer.db.ForkInserted
import org.ergoplatform.uexplorer.db.BestBlockInserted
import akka.stream.scaladsl.Source

import scala.collection.immutable.TreeSet

class BlockIndexer(
  backend: Backend,
  graphBackend: GraphBackend,
  blockHttpClient: BlockHttpClient,
  utxoState: MvUtxoState
)(implicit s: ActorSystem[Nothing], ps: ProtocolSettings, killSwitch: SharedKillSwitch)
  extends LazyLogging {

  private def forkDeleteFlow(parallelism: Int): Flow[Inserted, BestBlockInserted, NotUsed] =
    Flow[Inserted]
      .mapAsync(parallelism) {
        case ForkInserted(newBlocksInserted, supersededFork) =>
          backend
            .removeBlocksFromMainChain(supersededFork.map(_.headerId))
            .map(_ => newBlocksInserted)
        case bb: BestBlockInserted =>
          Future.successful(List(bb))
      }
      .mapConcat(identity)

  private val indexingSink: Sink[Height, Future[ChainSyncResult]] =
    Flow[Height]
      .via(blockHttpClient.blockFlow)
      .mapAsync(1) { block =>
        blockHttpClient
          .getBestBlockOrBranch(block, utxoState.containsBlock, List.empty)
          .map {
            case bestBlock :: Nil =>
              utxoState.addBestBlock(bestBlock)
            case winningFork =>
              utxoState.addWinningFork(winningFork)
          }
          .flatMap(Future.fromTry)
      }
      .via(forkDeleteFlow(1))
      .via(backend.blockWriteFlow)
      .wireTap { bb =>
        if (bb.block.header.height % MvUtxoState.MaxCacheSize * 2 == 0) {
          utxoState.flushCache()
        }
      }
      .async
      .buffer(100, OverflowStrategy.backpressure)
      .via(backend.addressWriteFlow(utxoState.getAddressStats))
      .async
      .buffer(100, OverflowStrategy.backpressure)
      .via(graphBackend.graphWriteFlow(utxoState.getAddressStats))
      .via(killSwitch.flow)
      .withAttributes(ActorAttributes.supervisionStrategy(Resiliency.decider))
      .toMat(Sink.lastOption[BestBlockInserted]) { case (_, lastBlockF) =>
        lastBlockF.map { lastBlock =>
          ChainSyncResult(
            lastBlock,
            utxoState,
            graphBackend.graphTraversalSource
          )
        }
      }

  def indexChain: Future[ChainSyncResult] =
    for
      bestBlockHeight <- blockHttpClient.getBestBlockHeight
      fromHeight = utxoState.getLastBlock.map(_._1).getOrElse(0) + 1
      _ = if (bestBlockHeight > fromHeight) logger.info(s"Going to index blocks from $fromHeight to $bestBlockHeight")
      _ = if (bestBlockHeight == fromHeight) logger.info(s"Going to index block $bestBlockHeight")
      syncResult <- Source(fromHeight to bestBlockHeight).runWith(indexingSink)
    yield syncResult

  def fixChain(missingHeights: TreeSet[Height]): Future[ChainSyncResult] =
    Source(missingHeights)
      .runWith(indexingSink)

}

object BlockIndexer {
  case class ChainSyncResult(
    lastBlock: Option[BestBlockInserted],
    utxoState: MvUtxoState,
    graphTraversalSource: GraphTraversalSource
  )
}
