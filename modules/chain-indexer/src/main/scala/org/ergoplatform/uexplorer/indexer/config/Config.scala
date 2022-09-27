package org.ergoplatform.uexplorer.indexer.config

import org.ergoplatform.explorer.settings.ProtocolSettings
import pureconfig.ConfigSource
import pureconfig.ConfigReader.Result
import pureconfig.generic.auto._
import org.ergoplatform.explorer.settings.pureConfigInstances._
import org.ergoplatform.uexplorer.indexer._
import org.ergoplatform.uexplorer.indexer.http.{LocalNodeUriMagnet, RemoteNodeUriMagnet}
import sttp.model.Uri

case class ChainIndexerConf(
  nodeAddressToInitFrom: Uri,
  peerAddressToPollFrom: Uri,
  backendType: BackendType,
  protocol: ProtocolSettings
) {
  def remoteUriMagnet: RemoteNodeUriMagnet = RemoteNodeUriMagnet(peerAddressToPollFrom)
  def localUriMagnet: LocalNodeUriMagnet   = LocalNodeUriMagnet(nodeAddressToInitFrom)

}

object ChainIndexerConf {

  lazy val loadDefaultOrThrow: ChainIndexerConf =
    ConfigSource.default.at("chain-indexer").loadOrThrow[ChainIndexerConf]

  lazy val loadWithFallback: Result[ChainIndexerConf] =
    ConfigSource
      .file("conf/chain-indexer.conf")
      .withFallback(ConfigSource.default)
      .at("chain-indexer")
      .load[ChainIndexerConf]

}

sealed trait BackendType
case object CassandraDb extends BackendType
case object InMemoryDb extends BackendType
