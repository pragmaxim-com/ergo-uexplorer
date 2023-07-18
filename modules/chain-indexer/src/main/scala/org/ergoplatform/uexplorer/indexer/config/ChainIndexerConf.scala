package org.ergoplatform.uexplorer.indexer.config

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.mining.emission.EmissionRules
import org.ergoplatform.settings.MonetarySettings
import org.ergoplatform.uexplorer.config.ExplorerConfig
import org.ergoplatform.uexplorer.{Address, NetworkPrefix, ProtocolSettings}
import org.ergoplatform.uexplorer.http.{LocalNodeUriMagnet, NodePoolConf, RemoteNodeUriMagnet}
import org.ergoplatform.uexplorer.storage.MvStorage.*
import org.ergoplatform.uexplorer.storage.MvStoreConf
import pureconfig.ConfigSource
import zio.config.*
import zio.config.magnolia.*
import zio.config.typesafe.*
import sttp.model.Uri
import zio.*

import java.io.File

@nameWithLabel("type")
sealed trait BackendType
case class H2(parallelism: Int) extends BackendType
case class Cassandra(parallelism: Int) extends BackendType

@nameWithLabel("type")
sealed trait GraphBackendType
case class JanusGraph(parallelism: Int) extends GraphBackendType
case class InMemoryGraph(parallelism: Int) extends GraphBackendType

case class ChainIndexerConf(
  mvStore: MvStoreConf,
  nodePool: NodePoolConf,
  backendType: BackendType,
  graphBackendType: GraphBackendType,
  protocol: ProtocolSettings,
  benchmarkMode: Boolean
)

object ChainIndexerConf extends LazyLogging {
  import NodePoolConf.*

  implicit val addressConfig: DeriveConfig[Address] =
    DeriveConfig[String].map(addr => Address.fromStringUnsafe(addr))

  implicit val networkConfig: DeriveConfig[NetworkPrefix] =
    DeriveConfig[String].map(prefix => NetworkPrefix.fromStringUnsafe(prefix))

  def config: zio.Config[ChainIndexerConf] = deriveConfig[ChainIndexerConf]

  def configIO: IO[zio.Config.Error, ChainIndexerConf] =
    ExplorerConfig().load[ChainIndexerConf](config)

  def layer: ZLayer[Any, zio.Config.Error, ChainIndexerConf] = ZLayer.fromZIO(configIO)

  /*
    def formatting(formatted: Boolean) = ConfigRenderOptions.concise().setFormatted(formatted).setJson(true)
    val chainIndexerConf =
      rootConfig.getValue("uexplorer.chain-indexer").render(formatting(true))
    logger.info(s"ChainIndexer config: $chainIndexerConf")
   */

}
