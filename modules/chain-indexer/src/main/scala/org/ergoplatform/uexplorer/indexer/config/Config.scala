package org.ergoplatform.uexplorer.indexer.config

import org.ergoplatform.explorer.settings.ProtocolSettings
import sttp.model.Uri

case class ChainIndexerConf(
  nodeAddressToInitFrom: Uri,
  peerAddressToPollFrom: Uri,
  backendType: BackendType,
  protocol: ProtocolSettings
)

sealed trait BackendType
case object ScyllaBackend extends BackendType
case object UnknownBackend extends BackendType
