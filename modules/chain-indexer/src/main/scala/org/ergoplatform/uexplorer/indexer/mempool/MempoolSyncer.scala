package org.ergoplatform.uexplorer.indexer.mempool

import akka.actor.typed.{ActorRef, ActorSystem}
import org.ergoplatform.uexplorer.indexer.chain.ChainState
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.mempool.MempoolStateHolder.{updateTransactions, MempoolStateChanges, UpdateTxs}

import scala.concurrent.Future

class MempoolSyncer(blockHttpClient: BlockHttpClient) {

  def syncMempool(
    chainState: ChainState
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[UpdateTxs]): Future[MempoolStateChanges] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    blockHttpClient.getBestBlockHeight.flatMap { bestBlockHeight =>
      if (chainState.blockBuffer.byHeight.lastOption.map(_._1).exists(_ >= bestBlockHeight)) {
        for {
          txs          <- blockHttpClient.getUnconfirmedTxs
          stateChanges <- updateTransactions(txs)
        } yield stateChanges
      } else {
        Future.successful(MempoolStateChanges(List.empty))
      }
    }
  }

}
