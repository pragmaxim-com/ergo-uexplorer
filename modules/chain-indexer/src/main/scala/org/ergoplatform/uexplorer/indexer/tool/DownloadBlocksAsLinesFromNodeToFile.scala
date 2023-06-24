package org.ergoplatform.uexplorer.indexer.tool

import akka.actor.CoordinatedShutdown
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorSystem, Behavior}
import akka.stream.scaladsl.{Compression, FileIO, Source}
import akka.stream.*
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.http.{BlockHttpClient, LocalNodeUriMagnet, RemoteNodeUriMagnet}
import org.ergoplatform.uexplorer.indexer.chain.{BlockReader, StreamExecutor}
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf

import java.io.{PrintWriter, StringWriter}
import java.nio.file.Paths
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object DownloadBlocksAsLinesFromNodeToFile extends App with LazyLogging {

  private lazy val guardian: Behavior[Nothing] =
    Behaviors.setup[Nothing] { implicit ctx =>
      implicit val system: ActorSystem[Nothing] = ctx.system

      val targetFile = Paths.get(System.getProperty("user.home"), ".ergo-uexplorer", "ergo-chain.lines.gz")
      targetFile.toFile.getParentFile.mkdirs()
      val chainConf = ChainIndexerConf.loadDefaultOrThrow._1

      implicit val localNodeUriMagnet: LocalNodeUriMagnet   = chainConf.localUriMagnet
      implicit val remoteNodeUriMagnet: RemoteNodeUriMagnet = chainConf.remoteUriMagnet
      implicit val killSwitch: SharedKillSwitch             = KillSwitches.shared("uexplorer-kill-switch")

      for
        blockHttpClient <- BlockHttpClient.withNodePoolBackend
        blockReader = new BlockReader(blockHttpClient)
        bestBlockHeight <- blockHttpClient.getBestBlockHeight
        _ = println(s"Initiating download from 1 to $bestBlockHeight")
      yield blockReader
        .blockIdSource(1)
        .buffer(20000, OverflowStrategy.backpressure)
        .mapAsync(1)(blockHttpClient.getBlockForIdAsString)
        .buffer(20000, OverflowStrategy.backpressure)
        .map(line => ByteString(line + "\n"))
        .withAttributes(Attributes.asyncBoundary and Attributes.inputBuffer(0, 64))
        .via(Compression.gzip)
        .withAttributes(Attributes.asyncBoundary and Attributes.inputBuffer(0, 64))
        .runWith(FileIO.toPath(targetFile))
        .andThen {
          case Failure(ex) =>
            val sw = new StringWriter()
            ex.printStackTrace(new PrintWriter(sw))
            println(s"Shutting down due to unexpected error:\n$sw")
            CoordinatedShutdown.get(system).run(CoordinatedShutdown.ActorSystemTerminateReason)
          case Success(_) =>
            CoordinatedShutdown.get(system).run(CoordinatedShutdown.ActorSystemTerminateReason)
        }
      Behaviors.same
    }

  lazy val system: ActorSystem[Nothing] = ActorSystem[Nothing](guardian, "download-blocks-as-lines-from-node-to-file")
  Await.result(system.whenTerminated, Duration.Inf)

}
