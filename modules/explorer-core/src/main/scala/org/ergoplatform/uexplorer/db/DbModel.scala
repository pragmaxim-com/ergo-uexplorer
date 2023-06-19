package org.ergoplatform.uexplorer.db

import io.circe.Json
import org.ergoplatform.uexplorer.Const.Protocol
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.Const.Protocol.Emission

import scala.collection.immutable.ArraySeq

final case class AdProof(
  headerId: BlockId,
  proofBytes: HexString, // AVL+ tree path
  digest: HexString // tree root hash
)

final case class Asset(
  tokenId: TokenId,
  boxId: BoxId,
  headerId: BlockId,
  index: Int,
  amount: Long
)

case class Record(txId: TxId, boxId: BoxId, address: Address, value: Value)
final case class LightBlock(headerId: BlockId, inputBoxes: ArraySeq[Record], outputBoxes: ArraySeq[Record], info: BlockInfo)

final case class FullBlock(
  header: Header,
  extension: BlockExtension,
  adProofOpt: Option[AdProof],
  txs: ArraySeq[Transaction],
  inputs: ArraySeq[Input],
  dataInputs: ArraySeq[DataInput],
  outputs: ArraySeq[Output],
  assets: ArraySeq[Asset],
  registers: ArraySeq[BoxRegister],
  tokens: ArraySeq[Token]
)

final case class BlockExtension(
  headerId: BlockId,
  digest: HexString,
  fields: Json // dict
)

final case class BlockInfo(
  revision: Revision,
  parentId: BlockId,
  timestamp: Long,
  height: Int,
  blockSize: Int, // block size (bytes)
  blockCoins: Long, // total amount of nERGs in the block
  blockMiningTime: Long, // block mining time
  txsCount: Int, // number of txs in the block
  txsSize: Int, // total size of all transactions in this block (bytes)
  minerAddress: Address,
  minerReward: Long, // total amount of nERGs miner received from coinbase
  minerRevenue: Long, // total amount of nERGs miner received as a reward (coinbase + fee)
  blockFee: Long, // total amount of transaction fee in the block (nERG)
  blockChainTotalSize: Long, // cumulative blockchain size including this block
  totalTxsCount: Long, // total number of txs in all blocks in the chain
  totalCoinsIssued: Long, // amount of nERGs issued in the block
  totalMiningTime: Long, // mining time of all the blocks in the chain
  totalFees: Long, // total amount of nERGs all miners received as a fee
  totalMinersReward: Long, // total amount of nERGs all miners received as a reward for all time
  totalCoinsInTxs: Long, // total amount of nERGs in all blocks
  maxTxGix: Long, // Global index of the last transaction in the block
  maxBoxGix: Long // Global index of the last output in the last transaction in the block
) {
  def this() = this(
    0,
    Protocol.blockId,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    Emission.address,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0
  ) // kryo needs a no-arg constructor
}

final case class BoxRegister(
  id: RegisterId,
  boxId: BoxId,
  sigmaType: SigmaType,
  rawValue: HexString,
  renderedValue: String
)

final case class DataInput(
  boxId: BoxId,
  txId: TxId,
  headerId: BlockId,
  index: Int, // input index input within tx
  mainChain: Boolean
)

final case class Header(
  id: BlockId,
  parentId: BlockId,
  version: Byte,
  height: Int,
  nBits: Long,
  difficulty: BigDecimal,
  timestamp: Long,
  stateRoot: HexString,
  adProofsRoot: HexString,
  transactionsRoot: HexString,
  extensionHash: HexString,
  minerPk: HexString,
  w: HexString, // PoW one time PK
  n: HexString, // PoW nonce
  d: String, // PoW distance
  votes: String, // hex-encoded votes for a soft-fork and parameters
  mainChain: Boolean // chain status, `true` if this header resides in main chain.
)

final case class Input(
  boxId: BoxId,
  txId: TxId,
  headerId: BlockId,
  proofBytes: Option[HexString], // serialized and hex-encoded cryptographic proof
  extension: Json, // arbitrary key-value dictionary
  index: Short, // index  of the input in the transaction
  mainChain: Boolean // chain status, `true` if this input resides in main chain.
)

final case class Output(
  boxId: BoxId,
  txId: TxId,
  txIndex: Short, // index of the wrapping tx within a block
  headerId: BlockId,
  value: Long, // amount of nanoERG in thee corresponding box
  creationHeight: Int, // the height this output was created
  settlementHeight: Int, // the height this output got fixed in blockchain
  index: Short, // index of the output in the transaction
  globalIndex: Long,
  ergoTree: HexString, // serialized and hex-encoded ErgoTree
  ergoTreeTemplateHash: ErgoTreeTemplateHash, // hash of serialized and hex-encoded ErgoTree template
  address: Address, // an address derived from ergoTree
  timestamp: Long, // time output appeared in the blockchain
  mainChain: Boolean // chain status, `true` if this output resides in main chain
)

final case class Token(
  id: TokenId,
  boxId: BoxId,
  emissionAmount: Long,
  name: Option[String],
  description: Option[String],
  `type`: Option[TokenType],
  decimals: Option[Int]
)

final case class Transaction(
  id: TxId,
  headerId: BlockId,
  inclusionHeight: Int,
  isCoinbase: Boolean,
  timestamp: Long, // approx time output appeared in the blockchain
  size: Int, // transaction size in bytes
  index: Short, // index of a transaction inside a block
  globalIndex: Long,
  mainChain: Boolean
) {
  def numConfirmations(bestHeight: Int): Int = bestHeight - inclusionHeight + 1
}
