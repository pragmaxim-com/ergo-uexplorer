package org.ergoplatform.uexplorer.node

import derevo.circe.decoder
import derevo.derive
import org.ergoplatform.uexplorer.BlockId

/** A model mirroring BlockTransactions entity from Ergo node REST API.
  * See `BlockTransactions` in https://github.com/ergoplatform/ergo/blob/master/src/main/resources/api/openapi.yaml
  */
@derive(decoder)
final case class ApiBlockTransactions(
  headerId: BlockId,
  transactions: List[ApiTransaction]
)
