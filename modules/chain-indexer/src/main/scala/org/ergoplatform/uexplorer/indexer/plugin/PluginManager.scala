package org.ergoplatform.uexplorer.indexer.plugin

import akka.Done
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.Source
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.chain.ChainState
import org.ergoplatform.uexplorer.indexer.mempool.MempoolSyncer.MempoolStateChanges
import org.ergoplatform.uexplorer.plugin.Plugin
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateWithPool, UtxoStateWithoutPool}
import scala.jdk.CollectionConverters.*
import java.util.ServiceLoader
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

object PluginManager {

  def loadPlugins: Try[List[Plugin]] = Try(ServiceLoader.load(classOf[Plugin]).iterator().asScala.toList)

  def executePlugins(
    plugins: List[Plugin],
    chainState: ChainState,
    stateChanges: MempoolStateChanges,
    newBlockOpt: Option[Block]
  )(implicit actorSystem: ActorSystem[Nothing]): Future[Done] =
    Future.fromTry(chainState.utxoStateWithCurrentEpochBoxes).flatMap { utxoState =>
      val utxoStateWoPool = UtxoStateWithoutPool(utxoState.addressByUtxo, utxoState.utxosByAddress)
      val poolExecutionPlan =
        stateChanges.utxoStateTransitionByTx(utxoState).flatMap { case (newTx, utxoStateWithPool) =>
          plugins.map(p => (p, newTx, utxoStateWithPool))
        }
      val chainExecutionPlan = newBlockOpt.toList.flatMap(newBlock => plugins.map(_ -> newBlock))
      Source
        .fromIterator(() => poolExecutionPlan)
        .mapAsync(1) { case (plugin, newTx, utxoStateWithPool) =>
          plugin.processMempoolTx(
            newTx,
            utxoStateWoPool,
            UtxoStateWithPool(utxoStateWithPool.addressByUtxo, utxoStateWithPool.utxosByAddress)
          )
        }
        .run()
        .flatMap { _ =>
          Source(chainExecutionPlan)
            .mapAsync(1) { case (plugin, newBlock) =>
              plugin.processNewBlock(newBlock, utxoStateWoPool)
            }
            .run()
        }
    }

}
