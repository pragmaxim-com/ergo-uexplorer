package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.{BoxId, TokenId, TokenType}

final case class Token(
  id: TokenId,
  boxId: BoxId,
  emissionAmount: Long,
  name: Option[String],
  description: Option[String],
  `type`: Option[TokenType],
  decimals: Option[Int]
)
