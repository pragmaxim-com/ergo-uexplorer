package org.ergoplatform.uexplorer.indexer

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.api.{Backend, InMemoryBackend}
import org.ergoplatform.uexplorer.indexer.cassandra.CassandraBackend
import org.ergoplatform.uexplorer.indexer.chain.ChainIndexer.ChainSyncResult
import org.ergoplatform.uexplorer.indexer.chain.ChainLoader.{ChainValid, MissingEpochs}
import org.ergoplatform.uexplorer.indexer.chain.ChainStateHolder.*
import org.ergoplatform.uexplorer.indexer.chain.{ChainIndexer, ChainLoader, ChainState, ChainStateHolder, Epoch}
import org.ergoplatform.uexplorer.indexer.config.{CassandraDb, ChainIndexerConf, InMemoryDb, ProtocolSettings}
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.mempool.{MempoolStateHolder, MempoolSyncer}
import org.ergoplatform.uexplorer.indexer.mempool.MempoolStateHolder.*
import org.ergoplatform.uexplorer.indexer.plugin.PluginManager
import org.ergoplatform.uexplorer.indexer.utxo.{DiskUtxoSnapshotManager, UtxoState}
import org.ergoplatform.uexplorer.plugin.Plugin
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateWithPool, UtxoStateWithoutPool}
import org.ergoplatform.uexplorer.{Address, BoxId, Const}

import java.util.ServiceLoader
import scala.collection.immutable.{ArraySeq, ListMap}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

object Application extends App with AkkaStreamSupport with LazyLogging {
  ChainIndexerConf.loadWithFallback match {
    case Left(failures) =>
      failures.toList.foreach(f => logger.error(s"Config error ${f.description} at ${f.origin}"))
      System.exit(1)
    case Right((conf, config)) =>
      val guardian: Behavior[Nothing] =
        Behaviors.setup[Nothing] { implicit ctx =>
          implicit val system: ActorSystem[Nothing] = ctx.system
          implicit val protocol: ProtocolSettings   = conf.protocol
          implicit val chainStateHolderRef: ActorRef[ChainStateHolderRequest] =
            ctx.spawn(new ChainStateHolder().initialBehavior, "ChainStateHolder")
          implicit val mempoolStateHolderRef: ActorRef[MempoolStateHolderRequest] =
            ctx.spawn(MempoolStateHolder.behavior(MempoolState.empty), "MempoolStateHolder")
          val initializationF =
            for {
              blockHttpClient <- BlockHttpClient.withNodePoolBackend(conf)
              pluginManager   <- PluginManager.initialize
              backend         <- Future.fromTry(Backend(conf.backendType))
              snapshotManager = new DiskUtxoSnapshotManager()
              chainIndexer    = new ChainIndexer(backend, blockHttpClient, snapshotManager)
              mempoolSyncer   = new MempoolSyncer(blockHttpClient)
              chainLoader     = new ChainLoader(backend, snapshotManager)
              scheduler       = new Scheduler(pluginManager, chainIndexer, mempoolSyncer, chainLoader)
              done <- scheduler.validateAndSchedule(0.seconds, 5.seconds)
            } yield done

          initializationF.andThen {
            case Failure(ex) =>
              logger.error("Shutting down due to unexpected error", ex)
              system.terminate()
            case Success(_) =>
              logger.error("Chain indexer should never stop successfully")
              system.terminate()
          }
          Behaviors.same
        }
      val system: ActorSystem[Nothing] = ActorSystem[Nothing](guardian, "uexplorer", config)
      Await.result(system.whenTerminated, Duration.Inf)
  }

}
