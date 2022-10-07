package org.ergoplatform.uexplorer.node

import derevo.circe.decoder
import derevo.derive
import org.ergoplatform.uexplorer.{BlockId, HexString}

/** A model mirroring AdProof entity from Ergo node REST API.
  * See `BlockADProofs` in https://github.com/ergoplatform/ergo/blob/master/src/main/resources/api/openapi.yaml
  */
@derive(decoder)
final case class ApiAdProof(
  headerId: BlockId,
  proofBytes: HexString,
  digest: HexString
)
