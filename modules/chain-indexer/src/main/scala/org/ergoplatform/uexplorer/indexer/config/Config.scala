package org.ergoplatform.uexplorer.indexer.config

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.settings.ProtocolSettings
import pureconfig.ConfigSource
import pureconfig.ConfigReader.Result
import pureconfig.generic.auto._
import org.ergoplatform.explorer.settings.pureConfigInstances._
import org.ergoplatform.uexplorer.indexer._
import org.ergoplatform.uexplorer.indexer.http.{LocalNodeUriMagnet, RemoteNodeUriMagnet}
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
