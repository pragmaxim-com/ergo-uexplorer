package org.ergoplatform.uexplorer.node

import derevo.circe._
import derevo.derive
import org.ergoplatform.uexplorer.{HexString, SigmaType}

@derive(encoder, decoder)
final case class ExpandedRegister(
  serializedValue: HexString,
  sigmaType: Option[SigmaType],
  renderedValue: Option[String]
)

@derive(encoder, decoder)
final case class ExpandedLegacyRegister(
  rawValue: HexString,
  valueType: String,
  decodedValue: String
)
