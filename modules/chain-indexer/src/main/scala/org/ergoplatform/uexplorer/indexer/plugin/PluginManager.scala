package org.ergoplatform.uexplorer.indexer.plugin

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.chain.ChainState
import org.ergoplatform.uexplorer.indexer.mempool.MempoolStateHolder.MempoolStateChanges
import org.ergoplatform.uexplorer.plugin.Plugin
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateWithPool, UtxoStateWithoutPool}

import scala.jdk.CollectionConverters.*
import java.util.ServiceLoader
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

class PluginManager(plugins: List[Plugin]) {

  def close(): Future[Unit] = Future.sequence(plugins.map(_.close)).map(_ => ())

  def executePlugins(
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

object PluginManager extends LazyLogging {
  def loadPlugins: Try[List[Plugin]] = Try(ServiceLoader.load(classOf[Plugin]).iterator().asScala.toList)

  def initialize(implicit system: ActorSystem[Nothing]): Future[PluginManager] =
    Future.fromTry(loadPlugins).flatMap { plugins =>
      if (plugins.nonEmpty) logger.info(s"Plugins loaded: ${plugins.map(_.name).mkString(", ")}")
      Future
        .sequence(plugins.map(_.init))
        .map { _ =>
          val pluginManager = new PluginManager(plugins)
          CoordinatedShutdown(system).addTask(
            CoordinatedShutdown.PhaseBeforeServiceUnbind,
            "stop-plugin-manager"
          ) { () =>
            pluginManager.close().map(_ => Done)
          }
          logger.info("Plugins initialized")
          pluginManager
        }
    }
}
