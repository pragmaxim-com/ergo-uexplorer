package org.ergoplatform.uexplorer.plugin.alert

import discord4j.common.util.Snowflake
import discord4j.core.`object`.entity.Message
import discord4j.core.`object`.entity.channel.MessageChannel
import discord4j.core.{DiscordClient, GatewayDiscordClient}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.node.ApiTransaction
import org.ergoplatform.uexplorer.plugin.Plugin
import org.ergoplatform.uexplorer.plugin.Plugin.{UtxoStateWithPool, UtxoStateWithoutPool}
import org.ergoplatform.uexplorer.*
import org.slf4j.{Logger, LoggerFactory}
import reactor.core.publisher.{Flux, Mono}
import retry.Policy

import scala.collection.immutable.ListMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*
import scala.util.Try

class AlertPlugin extends Plugin {
  protected val logger: Logger         = LoggerFactory.getLogger(getClass.getName)
  private val discord: Future[Discord] = Discord.fromEnv

  private lazy val detectors = List(
    new HighValueDetector(3 * 1000, 10 * 1000)
  )

  private lazy val trackers = List(
    new SourceAnalyzer()
  )

  def name: String = "Alert Plugin"

  def init: Future[Unit] = discord.map(_ => ())

  def close: Future[Unit] = discord.flatMap(_.logout)

  def processMempoolTx(
    newTx: ApiTransaction,
    utxoStateWoPool: UtxoStateWithoutPool,
    utxoStateWithPool: UtxoStateWithPool,
    graphTraversalSource: GraphTraversalSource,
    topAddresses: SortedTopAddressMap
  ): Future[Unit] =
    discord.flatMap { c =>
      c.sendMessages(
        detectors.flatMap { detector =>
          detector
            .inspectNewPoolTx(newTx, utxoStateWoPool, utxoStateWithPool, graphTraversalSource, topAddresses)
            .flatMap { txMatch =>
              trackers.flatMap(
                _.trackTx(txMatch, utxoStateWoPool, utxoStateWithPool, graphTraversalSource, topAddresses).toList
                  .map(_.toString)
              )
            }
        }
      )
    }

  def processNewBlock(
    newBlock: Block,
    utxoStateWoPool: UtxoStateWithoutPool,
    graphTraversalSource: GraphTraversalSource,
    topAddresses: SortedTopAddressMap
  ): Future[Unit] =
    discord.flatMap { c =>
      c.sendMessages(
        detectors.flatMap { detector =>
          detector
            .inspectNewBlock(newBlock, utxoStateWoPool, graphTraversalSource, topAddresses)
            .flatMap { blockMatch =>
              trackers.flatMap(
                _.trackBlock(blockMatch, utxoStateWoPool, graphTraversalSource, topAddresses).toList.map(_.toString)
              )
            }

        }
      )
    }
}
