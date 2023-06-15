package org.ergoplatform.uexplorer.plugin.alert

import scala.concurrent.ExecutionContext.Implicits.global

import discord4j.common.util.Snowflake
import discord4j.core.{DiscordClient, GatewayDiscordClient}
import discord4j.core.`object`.entity.Message
import discord4j.core.`object`.entity.channel.MessageChannel
import org.ergoplatform.uexplorer.db.FullBlock
import org.ergoplatform.uexplorer.{Address, BoxId, TxId}
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.Plugin
import org.slf4j.{Logger, LoggerFactory}
import reactor.core.publisher.{Flux, Mono}
import retry.Policy

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.*
import scala.concurrent.Future
import scala.jdk.FutureConverters.*
import scala.util.Try
import scala.concurrent.duration.*

class Discord(discordAlertChannelId: String, client: GatewayDiscordClient) {
  protected val logger: Logger    = LoggerFactory.getLogger(getClass.getName)
  private val retryPolicy: Policy = retry.Backoff(3, 3.second)

  def logout: Future[Unit] = client.logout().toFuture.asScala.map(_ => ())

  // message can have max 2000 characters
  def sendMessages(msgs: List[String]): Future[Unit] =
    if (msgs.isEmpty)
      Future.successful(())
    else
      retryPolicy.apply { () =>
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
          .toFuture
          .asScala
          .map(_ => ())
      }(retry.Success.always, global)

}

object Discord {

  def fromEnv: Future[Discord] =
    Future
      .fromTry(
        Try {
          Option(System.getenv("DISCORD_TOKEN"))
            .filterNot(_ == "")
            .getOrElse(throw new IllegalStateException("DISCORD_TOKEN must be exported"))
        }
      )
      .flatMap { discordToken =>
        DiscordClient
          .builder(discordToken)
          .build
          .login
          .toFuture
          .asScala
      }
      .map { client =>
        val discordAlertChannelId =
          Option(System.getenv("DISCORD_ALERT_CHANNEL"))
            .filterNot(_ == "")
            .getOrElse(throw new IllegalStateException("DISCORD_ALERT_CHANNEL must be exported"))
        new Discord(discordAlertChannelId, client)
      }
}
