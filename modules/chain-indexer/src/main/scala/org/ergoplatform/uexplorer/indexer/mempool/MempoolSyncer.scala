package org.ergoplatform.uexplorer.indexer.mempool

import akka.actor.typed.{ActorRef, ActorSystem}
import org.ergoplatform.uexplorer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.mempool.MempoolStateHolder
import org.ergoplatform.uexplorer.indexer.mempool.MempoolStateHolder.{MempoolStateChanges, UpdateTxs}
import org.ergoplatform.uexplorer.utxo.MvUtxoState

import scala.concurrent.Future

class MempoolSyncer(blockHttpClient: BlockHttpClient) {

  def syncMempool(
    utxoState: MvUtxoState
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[UpdateTxs]): Future[MempoolStateChanges] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    blockHttpClient.getBestBlockHeight.flatMap { bestBlockHeight =>
      if (utxoState.getLastBlock.map(_._1).exists(_ >= bestBlockHeight)) {
        for {
          txs          <- blockHttpClient.getUnconfirmedTxs
          stateChanges <- MempoolStateHolder.updateTransactions(txs)
        } yield stateChanges
      } else {
        Future.successful(MempoolStateChanges(List.empty))
      }
    }
  }

}
