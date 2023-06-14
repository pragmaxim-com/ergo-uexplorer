package org.ergoplatform.uexplorer.indexer.plugin

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.db.{BestBlockInserted, Block}
import org.ergoplatform.uexplorer.indexer.mempool.MempoolStateHolder.MempoolStateChanges
import org.ergoplatform.uexplorer.plugin.Plugin
import org.ergoplatform.uexplorer.{SortedTopAddressMap, Storage}

import java.util.ServiceLoader
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.Try

class PluginManager(plugins: List[Plugin]) extends LazyLogging {

  def close(): Future[Unit] = Future.sequence(plugins.map(_.close)).map(_ => ())

  def executePlugins(
    storage: Storage,
    stateChanges: MempoolStateChanges,
    graphTraversalSource: Option[GraphTraversalSource],
    newBlockOpt: Option[BestBlockInserted]
  )(implicit actorSystem: ActorSystem[Nothing]): Future[Done] = {
    val poolExecutionPlan =
      stateChanges.stateTransitionByTx.flatMap { case (newTx, _) =>
        plugins.map(p => (p, newTx))
      }
    val chainExecutionPlan = newBlockOpt.toList.flatMap(newBlock => plugins.map(_ -> newBlock))
    Source
      .fromIterator(() => poolExecutionPlan.iterator)
      .mapAsync(1) { case (plugin, newTx) =>
        plugin.processMempoolTx(
          newTx,
          storage,
          graphTraversalSource
        )
      }
      .run()
      .flatMap { _ =>
        Source(chainExecutionPlan)
          .mapAsync(1) { case (plugin, newBlock) =>
            plugin.processNewBlock(newBlock, storage, graphTraversalSource)
          }
          .run()
      }
      .recover { case ex: Throwable =>
        logger.error("Plugin failure should not propagate upstream", ex)
        Done
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
            CoordinatedShutdown.PhaseServiceStop,
            "stop-plugin-manager"
          ) { () =>
            pluginManager.close().map(_ => Done)
          }
          logger.info("Plugins initialized")
          pluginManager
        }
    }
}
