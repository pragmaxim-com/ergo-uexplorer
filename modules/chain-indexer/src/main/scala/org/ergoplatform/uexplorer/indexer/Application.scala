package org.ergoplatform.uexplorer.indexer

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import pureconfig.ConfigSource
import pureconfig._
import pureconfig.generic.auto._
import org.ergoplatform.explorer.settings.pureConfigInstances._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Application extends App with LazyLogging {
  ConfigSource
    .file("../conf/chain-indexer.conf")
    .withFallback(ConfigSource.default)
    .at("chain-indexer")
    .load[ChainIndexerConf] match {
    case Left(failures) =>
      failures.toList.foreach(f => logger.error(s"Config error ${f.description} at ${f.location}"))
      System.exit(1)
    case Right(conf) =>
      val guardian: Behavior[Nothing] =
        Behaviors.setup[Nothing] { implicit ctx =>
          Indexer.runWith(conf)
          Behaviors.same
        }

      val system: ActorSystem[Nothing] = ActorSystem[Nothing](guardian, "uexplorer")
      Await.result(system.whenTerminated, Duration.Inf)
  }

}
