package org.ergoplatform.uexplorer.node

import io.circe.Json
import org.ergoplatform.uexplorer.*

import scala.collection.immutable.ArraySeq
import scala.util.{Failure, Success, Try}

final case class ApiAdProof(
  headerId: BlockId,
  proofBytes: HexString,
  digest: HexString
)

final case class ApiAsset(
  tokenId: TokenId,
  amount: Long
)

final case class ApiBlockExtension(
  headerId: BlockId,
  digest: HexString,
  fields: Json
)

final case class ApiBlockTransactions(
  headerId: BlockId,
  transactions: ArraySeq[ApiTransaction]
)

final case class ApiDataInput(boxId: BoxId)

final case class ApiDifficulty(value: BigDecimal)

final case class ApiFullBlock(
  header: ApiHeader,
  transactions: ApiBlockTransactions,
  extension: ApiBlockExtension,
  adProofs: Option[ApiAdProof],
  size: Int
)

final case class ApiHeader(
  id: BlockId,
  parentId: BlockId,
  version: Byte,
  height: Int,
  nBits: Long,
  difficulty: ApiDifficulty,
  timestamp: Long,
  stateRoot: HexString,
  adProofsRoot: HexString,
  transactionsRoot: HexString,
  extensionHash: HexString,
  minerPk: HexString,
  w: HexString,
  n: HexString,
  d: String,
  votes: String
)

final case class ApiOutput(
  boxId: BoxId,
  value: Long,
  creationHeight: Int,
  ergoTree: HexString,
  assets: List[ApiAsset],
  additionalRegisters: Map[RegisterId, HexString]
)

final case class ApiPowSolutions(pk: HexString, w: HexString, n: HexString, d: String)

final case class ApiSpendingProof(proofBytes: Option[HexString], extension: Json)

final case class ApiInput(boxId: BoxId, spendingProof: ApiSpendingProof)

final case class ApiTransaction(
  id: TxId,
  inputs: ArraySeq[ApiInput],
  dataInputs: List[ApiDataInput],
  outputs: ArraySeq[ApiOutput],
  size: Int
)

final case class ExpandedRegister(
  serializedValue: HexString,
  regValue: Option[RegisterValue]
)

final case class RegisterValue(sigmaType: SigmaType, value: String)

final case class TokenProps(
  name: String,
  description: String,
  decimals: Int
)
