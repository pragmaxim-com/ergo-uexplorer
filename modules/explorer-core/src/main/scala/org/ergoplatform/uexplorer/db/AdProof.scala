package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.{BlockId, HexString}

/** Represents `node_ad_proofs` table.
  */
final case class AdProof(
  headerId: BlockId,
  proofBytes: HexString, // serialized and hex-encoded AVL+ tree path
  digest: HexString // hex-encoded tree root hash
)
