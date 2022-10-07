package org.ergoplatform.uexplorer.node

import derevo.circe.decoder
import derevo.derive
import io.circe.Json
import org.ergoplatform.uexplorer.{BlockId, HexString}

/** A model mirroring Extension entity from Ergo node REST API.
  * See `Extension` in https://github.com/ergoplatform/ergo/blob/master/src/main/resources/api/openapi.yaml
  */
@derive(decoder)
final case class ApiBlockExtension(
  headerId: BlockId,
  digest: HexString,
  fields: Json
)
