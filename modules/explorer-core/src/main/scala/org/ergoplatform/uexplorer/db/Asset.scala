package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.{BlockId, BoxId, TokenId}

/** Represents `node_assets` table. */
final case class Asset(
  tokenId: TokenId,
  boxId: BoxId,
  headerId: BlockId,
  index: Int,
  amount: Long
)
