package org.ergoplatform.uexplorer.indexer.config

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.mining.emission.EmissionRules
import org.ergoplatform.settings.MonetarySettings
import org.ergoplatform.uexplorer.config.ExplorerConfig
import org.ergoplatform.uexplorer.{Address, CoreConf, NetworkPrefix}
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
  protocol: CoreConf,
  benchmarkMode: Boolean
)

object ChainIndexerConf {
  import NodePoolConf.*

  implicit val addressConfig: DeriveConfig[Address] =
    DeriveConfig[String].map(addr => Address.fromStringUnsafe(addr))

  implicit val networkConfig: DeriveConfig[NetworkPrefix] =
    DeriveConfig[String].map(prefix => NetworkPrefix.fromStringUnsafe(prefix))

  def config: zio.Config[ChainIndexerConf] = deriveConfig[ChainIndexerConf]

  def configIO: ZIO[Any, Throwable, ChainIndexerConf] =
    for {
      provider <- ExplorerConfig.provider(debugLog = true)
      conf     <- provider.load[ChainIndexerConf](config)
    } yield conf

  def layer: ZLayer[Any, Throwable, ChainIndexerConf] = ZLayer.fromZIO(configIO)

}
