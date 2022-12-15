package org.ergoplatform.uexplorer.plugin.alert

import discord4j.common.util.Snowflake
import discord4j.core.DiscordClient
import discord4j.core.`object`.entity.Message
import discord4j.core.`object`.entity.channel.MessageChannel
import org.ergoplatform.uexplorer.{Address, BoxId, TxId}
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.Plugin
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateWithPool, UtxoStateWithoutPool}
import org.slf4j.{Logger, LoggerFactory}
import reactor.core.publisher.{Flux, Mono}
import retry.Policy

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.FutureConverters.*
import scala.util.Try
import scala.concurrent.duration.*

class AlertPlugin extends Plugin {
  protected val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  private val retryPolicy: Policy = retry.Backoff(3, 3.second)

  private var discordToken: String          = null
  private var discordAlertChannelId: String = null

  // message can have max 2000 characters
  private def sendMessages(msgs: List[String]): Future[Unit] =
    if (msgs.isEmpty)
      Future.successful(())
    else
      retryPolicy.apply { () =>
        DiscordClient
          .builder(discordToken)
          .build
          .login
          .flux()
          .flatMap { client =>
            client
              .getChannelById(Snowflake.of(discordAlertChannelId))
              .ofType(classOf[MessageChannel])
              .flux()
              .flatMap { channel =>
                Mono
                  .just(msgs.asJava)
                  .flatMapMany(Flux.fromIterable)
                  .flatMap { msg =>
                    logger.info(msg)
                    channel.createMessage(msg)
                  }
              }
              .collectList()
              .doFinally(_ => client.logout())
          }
          .count()
          .toFuture
          .asScala
          .map(_ => ())
      }(retry.Success.always, global)

  private lazy val detectors = List(new HighTxValueDetector(20 * 1000 * 1000000000L))

  def name: String = "Alert Plugin"

  def init: Try[Unit] = Try {
    discordToken = Option(System.getenv("DISCORD_TOKEN"))
      .filterNot(_ == "")
      .getOrElse(throw new IllegalStateException("DISCORD_TOKEN must be exported"))
    discordAlertChannelId = Option(System.getenv("DISCORD_ALERT_CHANNEL"))
      .filterNot(_ == "")
      .getOrElse(throw new IllegalStateException("DISCORD_ALERT_CHANNEL must be exported"))
  }

  def execute(
    newTx: ApiTransaction,
    utxoStateWoPool: UtxoStateWithoutPool,
    utxoStateWithPool: UtxoStateWithPool
  ): Future[Unit] =
    sendMessages(
      detectors.flatMap { decoder =>
        decoder.inspect(newTx, utxoStateWoPool, utxoStateWithPool)
      }
    )
}
