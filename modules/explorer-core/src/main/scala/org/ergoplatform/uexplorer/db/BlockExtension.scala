package org.ergoplatform.uexplorer.db

import io.circe.Json
import org.ergoplatform.uexplorer.{BlockId, HexString}

/** Represents `node_extensions` table. */
final case class BlockExtension(
  headerId: BlockId,
  digest: HexString,
  fields: Json // arbitrary key->value dictionary
)
