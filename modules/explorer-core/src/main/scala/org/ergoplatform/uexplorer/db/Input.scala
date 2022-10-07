package org.ergoplatform.uexplorer.db

import io.circe.Json
import org.ergoplatform.uexplorer.{BlockId, BoxId, HexString, TxId}

/** Represents `node_inputs` table.
  */
final case class Input(
  boxId: BoxId,
  txId: TxId,
  headerId: BlockId,
  proofBytes: Option[HexString], // serialized and hex-encoded cryptographic proof
  extension: Json, // arbitrary key-value dictionary
  index: Int, // index  of the input in the transaction
  mainChain: Boolean // chain status, `true` if this input resides in main chain.
)
