package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.{BoxId, HexString, RegisterId, SigmaType}

final case class BoxRegister(
  id: RegisterId,
  boxId: BoxId,
  sigmaType: SigmaType,
  rawValue: HexString,
  renderedValue: String
)
