package org.ergoplatform.uexplorer.indexer.config

import cats.data.NonEmptyList
import cats.syntax.list._
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import eu.timepit.refined.api.{Refined, Validate}
import eu.timepit.refined.refineV
import org.ergoplatform.uexplorer.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.http.{LocalNodeUriMagnet, RemoteNodeUriMagnet}
import pureconfig.ConfigReader.Result
import pureconfig.error.CannotConvert
import pureconfig.generic.auto._
import pureconfig.{ConfigReader, ConfigSource}
import sttp.model.Uri

import java.io.File

case class ChainIndexerConf(
  nodeAddressToInitFrom: Uri,
  peerAddressToPollFrom: Uri,
  backendType: BackendType,
  protocol: ProtocolSettings
) {
  def remoteUriMagnet: RemoteNodeUriMagnet = RemoteNodeUriMagnet(peerAddressToPollFrom)
  def localUriMagnet: LocalNodeUriMagnet   = LocalNodeUriMagnet(nodeAddressToInitFrom)
}

object ChainIndexerConf extends LazyLogging {

  implicit def configReaderForRefined[A: ConfigReader, P](implicit
    v: Validate[A, P]
  ): ConfigReader[A Refined P] =
    ConfigReader[A].emap { a =>
      refineV[P](a).left.map(r => CannotConvert(a.toString, s"Refined", r))
    }

  implicit def nelReader[A: ConfigReader]: ConfigReader[NonEmptyList[A]] =
    implicitly[ConfigReader[List[A]]].emap { list =>
      list.toNel.toRight(CannotConvert(list.toString, s"NonEmptyList", "List is empty"))
    }

  implicit def uriConfigReader(implicit cr: ConfigReader[String]): ConfigReader[Uri] =
    cr.emap(addr => Uri.parse(addr).left.map(r => CannotConvert(addr, "Uri", r)))

  lazy val loadDefaultOrThrow: ChainIndexerConf =
    ConfigSource.default.at("uexplorer.chain-indexer").loadOrThrow[ChainIndexerConf]

  lazy val loadWithFallback: Result[(ChainIndexerConf, Config)] = {
    def formatting(formatted: Boolean) = ConfigRenderOptions.concise().setFormatted(formatted).setJson(true)
    val rootConfig =
      ConfigFactory
        .parseFile(new File("conf/chain-indexer.conf"))
        .withFallback(ConfigFactory.load())
        .resolve()

    val chainIndexerConf =
      rootConfig.getValue("uexplorer.chain-indexer").render(formatting(true))
    val cassandraContactPoints =
      rootConfig.getValue("datastax-java-driver.basic.contact-points").render(formatting(false))
    logger.info(s"ChainIndexer config: $chainIndexerConf")
    logger.info(s"Cassandra contact points: $cassandraContactPoints")

    ConfigSource
      .file("conf/chain-indexer.conf")
      .withFallback(ConfigSource.default)
      .at("uexplorer.chain-indexer")
      .load[ChainIndexerConf]
      .map(_ -> rootConfig)
  }
}

sealed trait BackendType

case class CassandraDb(parallelism: Int) extends BackendType
case object InMemoryDb extends BackendType
