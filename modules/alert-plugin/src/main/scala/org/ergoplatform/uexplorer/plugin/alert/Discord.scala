package org.ergoplatform.uexplorer.plugin.alert

import scala.concurrent.ExecutionContext.Implicits.global
import discord4j.common.util.Snowflake
import discord4j.core.{DiscordClient, GatewayDiscordClient}
import discord4j.core.`object`.entity.Message
import discord4j.core.`object`.entity.channel.MessageChannel
import org.ergoplatform.uexplorer.db.FullBlock
import org.ergoplatform.uexplorer.{BoxId, ErgoTreeHex, TxId}
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.Plugin
import org.slf4j.{Logger, LoggerFactory}
import reactor.core.publisher.{Flux, Mono}
import retry.Policy
import zio.*
import zio.stream.ZStream

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.*
import scala.concurrent.Future
import scala.jdk.FutureConverters.*
import scala.util.Try
import scala.concurrent.duration.*

class Discord(discordAlertChannelId: String, client: GatewayDiscordClient) {
  protected val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def logout: Task[Unit] = ZIO.fromFuture(_ => client.logout().toFuture.asScala.map(_ => ()))

  // message can have max 2000 characters
  def sendMessages(msgs: List[String]): Task[Unit] =
    if (msgs.isEmpty)
      ZIO.unit
    else
      ZIO
        .fromFuture { implicit exC =>
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
        }
        .retry(Schedule.exponential(1.seconds, 2.0).upTo(1.minute))

}

object Discord {

  def fromEnv: Task[Discord] =
    ZIO
      .fromTry(
        Try {
          Option(java.lang.System.getenv("DISCORD_TOKEN"))
            .filterNot(_ == "")
            .getOrElse(throw new IllegalStateException("DISCORD_TOKEN must be exported"))
        }
      )
      .flatMap { discordToken =>
        ZIO.fromFuture { implicit exC =>
          DiscordClient
            .builder(discordToken)
            .build
            .login
            .toFuture
            .asScala
        }
      }
      .map { client =>
        val discordAlertChannelId =
          Option(java.lang.System.getenv("DISCORD_ALERT_CHANNEL"))
            .filterNot(_ == "")
            .getOrElse(throw new IllegalStateException("DISCORD_ALERT_CHANNEL must be exported"))
        new Discord(discordAlertChannelId, client)
      }


  for {
    promise <- Promise.make[Nothing, Unit]
    hub <- Hub.bounded[String](2)
    scoped = ZStream.fromHubScoped(hub).tap(_ => promise.succeed(()))
    stream = ZStream.unwrapScoped(scoped)
    fiber <- stream.take(2).runCollect.fork
    _ <- promise.await
    _ <- hub.publish("Hello")
    _ <- hub.publish("World")
    _ <- fiber.join
  } yield ()
}
