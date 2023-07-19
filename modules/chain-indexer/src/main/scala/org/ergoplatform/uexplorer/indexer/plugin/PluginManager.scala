package org.ergoplatform.uexplorer.indexer.plugin

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.ReadableStorage
import org.ergoplatform.uexplorer.db.{BestBlockInserted, FullBlock}
import org.ergoplatform.uexplorer.plugin.Plugin
import zio.*
import zio.stream.ZStream
import java.util.ServiceLoader
import scala.jdk.CollectionConverters.*
import scala.util.Try
import org.ergoplatform.uexplorer.indexer.mempool.MemPoolStateChanges
import zio.stream.ZSink

case class PluginManager(plugins: List[Plugin]) {

  def close(): Task[Unit] = ZIO.collectAllDiscard(plugins.map(_.close))

  def executePlugins(
    storage: ReadableStorage,
    stateChanges: MemPoolStateChanges,
    graphTraversalSource: GraphTraversalSource,
    newBlockOpt: Option[BestBlockInserted]
  ): Task[Long] = {
    val poolExecutionPlan =
      stateChanges.stateTransitionByTx.flatMap { case (newTx, _) =>
        plugins.map(p => (p, newTx))
      }
    val chainExecutionPlan = newBlockOpt.toList.flatMap(newBlock => plugins.map(_ -> newBlock))
    ZStream
      .fromIterator(poolExecutionPlan.iterator)
      .mapZIO { case (plugin, newTx) =>
        plugin.processMempoolTx(
          newTx,
          storage,
          graphTraversalSource
        )
      }
      .run(ZSink.count)
      .flatMap { _ =>
        ZStream
          .fromIterable(chainExecutionPlan)
          .mapZIO { case (plugin, newBlock) =>
            plugin.processNewBlock(newBlock, storage, graphTraversalSource)
          }
          .run(ZSink.count)
      }
      .logError("Plugin failure should not propagate upstream")
  }

}

object PluginManager {
  def loadPlugins: Task[List[Plugin]] = ZIO.attempt(ServiceLoader.load(classOf[Plugin]).iterator().asScala.toList)

  def layerNoPlugins: ZLayer[Any, Throwable, PluginManager] = ZLayer.succeed(PluginManager(List.empty))

  def layer: ZLayer[Any, Throwable, PluginManager] = ZLayer.scoped(
    loadPlugins.flatMap { plugins =>
      ZIO
        .collectAllParDiscard(plugins.map(_.init))
        .flatMap { _ =>
          ZIO.acquireRelease(ZIO.succeed(PluginManager(plugins)))(p => ZIO.succeed(p.close()))
        } <* ZIO.when(plugins.nonEmpty)(ZIO.log(s"Plugins loaded: ${plugins.map(_.name).mkString(", ")}"))
    }
  )
}
