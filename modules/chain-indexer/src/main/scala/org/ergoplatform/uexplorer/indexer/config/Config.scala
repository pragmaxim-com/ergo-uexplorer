package org.ergoplatform.uexplorer.indexer.config

import eu.timepit.refined.api.Refined
import org.ergoplatform.explorer.settings.ProtocolSettings
import pureconfig.ConfigSource
import pureconfig.ConfigReader.Result
import eu.timepit.refined.string.Uri
import pureconfig.generic.auto._
import org.ergoplatform.explorer.settings.pureConfigInstances._
import sttp.model

case class ChainIndexerConf(
  nodeAddressToInitFrom: String Refined Uri,
  peerAddressToPollFrom: String Refined Uri,
  backendType: BackendType,
  protocol: ProtocolSettings
) {
  // helper methods due to pureconfig http4s implicits not working
  def nodeUriToInitFrom: model.Uri = sttp.model.Uri.unsafeParse(nodeAddressToInitFrom.toString)
  def peerUriToPollFrom: model.Uri = sttp.model.Uri.unsafeParse(peerAddressToPollFrom.toString)
}

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
