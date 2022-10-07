package org.ergoplatform.uexplorer.db

final case class Block(
  header: Header,
  extension: BlockExtension,
  adProofOpt: Option[AdProof],
  txs: List[Transaction],
  inputs: List[Input],
  dataInputs: List[DataInput],
  outputs: List[Output],
  assets: List[Asset],
  registers: List[BoxRegister],
  tokens: List[Token],
  info: BlockInfo,
)
