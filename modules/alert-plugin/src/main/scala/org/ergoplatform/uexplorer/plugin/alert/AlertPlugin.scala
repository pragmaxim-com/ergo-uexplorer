package org.ergoplatform.uexplorer.plugin.alert

import discord4j.common.util.Snowflake
import discord4j.core.{DiscordClient, GatewayDiscordClient}
import discord4j.core.`object`.entity.Message
import discord4j.core.`object`.entity.channel.MessageChannel
import org.ergoplatform.uexplorer.db.Block
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

  private var discordToken: String                 = null
  private var discordAlertChannelId: String        = null
  private var client: Future[GatewayDiscordClient] = null

  // message can have max 2000 characters
  private def sendMessages(msgs: List[String]): Future[Unit] =
    if (msgs.isEmpty)
      Future.successful(())
    else
      retryPolicy.apply { () =>
        client
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
              .toFuture
              .asScala
          }
          .map(_ => ())
      }(retry.Success.always, global)

  private lazy val detectors = List(
    new HighValueDetector(200 * 1000, 500 * 1000)
  )

  def name: String = "Alert Plugin"

  def init: Future[Unit] = Future
    .fromTry(
      Try {
        discordToken = Option(System.getenv("DISCORD_TOKEN"))
          .filterNot(_ == "")
          .getOrElse(throw new IllegalStateException("DISCORD_TOKEN must be exported"))
        discordAlertChannelId = Option(System.getenv("DISCORD_ALERT_CHANNEL"))
          .filterNot(_ == "")
          .getOrElse(throw new IllegalStateException("DISCORD_ALERT_CHANNEL must be exported"))
      }
    )
    .flatMap { _ =>
      client = DiscordClient
        .builder(discordToken)
        .build
        .login
        .toFuture
        .asScala
      client.map(_ => ())
    }

  def close: Future[Unit] =
    client.flatMap(_.logout().toFuture.asScala.map(_ => ()))

  def processMempoolTx(
    newTx: ApiTransaction,
    utxoStateWoPool: UtxoStateWithoutPool,
    utxoStateWithPool: UtxoStateWithPool
  ): Future[Unit] =
    sendMessages(
      detectors.flatMap { detector =>
        detector.inspectNewPoolTx(newTx, utxoStateWoPool, utxoStateWithPool)
      }
    )

  def processNewBlock(
    newBlock: Block,
    utxoStateWoPool: UtxoStateWithoutPool
  ): Future[Unit] =
    sendMessages(
      detectors.flatMap { detector =>
        detector.inspectNewBlock(newBlock, utxoStateWoPool)
      }
    )

}
