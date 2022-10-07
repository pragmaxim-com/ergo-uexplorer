package org.ergoplatform.uexplorer.indexer

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Application extends App with LazyLogging {
  ChainIndexerConf.loadWithFallback match {
    case Left(failures) =>
      failures.toList.foreach(f => logger.error(s"Config error ${f.description} at ${f.origin}"))
      System.exit(1)
    case Right((chainIndexerConf, config)) =>
      val guardian: Behavior[Nothing] =
        Behaviors.setup[Nothing] { implicit ctx =>
          Indexer.runWith(chainIndexerConf)
          Behaviors.same
        }
      val system: ActorSystem[Nothing] = ActorSystem[Nothing](guardian, "uexplorer", config)
      Await.result(system.whenTerminated, Duration.Inf)
  }

}
