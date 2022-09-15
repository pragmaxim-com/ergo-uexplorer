package org.ergoplatform.uexplorer.indexer.config

import org.ergoplatform.explorer.settings.ProtocolSettings
import pureconfig.ConfigSource
import pureconfig.ConfigReader.Result
import pureconfig.generic.auto._
import org.ergoplatform.explorer.settings.pureConfigInstances._
import org.ergoplatform.uexplorer.indexer._
import sttp.model.Uri

case class ChainIndexerConf(
  nodeAddressToInitFrom: Uri,
  peerAddressToPollFrom: Uri,
  backendType: BackendType,
  protocol: ProtocolSettings
)

object ChainIndexerConf {

  lazy val loadDefaultOrThrow: ChainIndexerConf =
    ConfigSource.default.at("chain-indexer").loadOrThrow[ChainIndexerConf]

  lazy val loadWithFallback: Result[ChainIndexerConf] =
    ConfigSource
      .file("../conf/chain-indexer.conf")
      .withFallback(ConfigSource.default)
      .at("chain-indexer")
      .load[ChainIndexerConf]

}

sealed trait BackendType
case object ScyllaBackend extends BackendType
case object UnknownBackend extends BackendType
