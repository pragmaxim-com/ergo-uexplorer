package org.ergoplatform.uexplorer.db

import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.Const.Protocol
import org.ergoplatform.uexplorer.Const.Protocol.Emission
import zio.*
import zio.json.*
import zio.json.interop.refined.*

import scala.collection.mutable

case class Box(
  boxId: BoxId,
  txId: TxId,
  creationHeight: CreationHeight,
  settlementHeight: SettlementHeight,
  ergoTreeHash: ErgoTreeHash,
  ergoTreeT8Hash: Option[ErgoTreeT8Hash],
  ergValue: Value,
  r4: Option[BoxRegisterValueHex],
  r5: Option[BoxRegisterValueHex],
  r6: Option[BoxRegisterValueHex],
  r7: Option[BoxRegisterValueHex],
  r8: Option[BoxRegisterValueHex],
  r9: Option[BoxRegisterValueHex]
)

object Box {
  implicit val encoder: JsonEncoder[Box] = DeriveJsonEncoder.gen[Box]
  implicit val decoder: JsonDecoder[Box] = DeriveJsonDecoder.gen[Box]
}

case class Utxo(
  boxId: BoxId,
  txId: TxId,
  creationHeight: CreationHeight,
  settlementHeight: SettlementHeight,
  ergoTreeHash: ErgoTreeHash,
  ergoTreeT8Hash: Option[ErgoTreeT8Hash],
  ergValue: Value,
  r4: Option[BoxRegisterValueHex],
  r5: Option[BoxRegisterValueHex],
  r6: Option[BoxRegisterValueHex],
  r7: Option[BoxRegisterValueHex],
  r8: Option[BoxRegisterValueHex],
  r9: Option[BoxRegisterValueHex]
) {
  def toBox: Box =
    Box(
      boxId,
      txId,
      creationHeight,
      settlementHeight,
      ergoTreeHash,
      ergoTreeT8Hash,
      ergValue,
      r4,
      r5,
      r6,
      r7,
      r8,
      r9
    )
}

object Utxo {
  implicit val encoder: JsonEncoder[Utxo] = DeriveJsonEncoder.gen[Utxo]
  implicit val decoder: JsonDecoder[Utxo] = DeriveJsonDecoder.gen[Utxo]
}

case class ErgoTree(hash: ErgoTreeHash, blockId: BlockId, hex: ErgoTreeHex)
case class ErgoTreeT8(hash: ErgoTreeT8Hash, blockId: BlockId, hex: ErgoTreeT8Hex)

case class Asset(tokenId: TokenId, blockId: BlockId)
object Asset {
  implicit val encoder: JsonEncoder[Asset] = DeriveJsonEncoder.gen[Asset]
  implicit val decoder: JsonDecoder[Asset] = DeriveJsonDecoder.gen[Asset]
}

case class Asset2Box(tokenId: TokenId, boxId: BoxId, amount: Amount)
object Asset2Box {
  implicit val encoder: JsonEncoder[Asset2Box] = DeriveJsonEncoder.gen[Asset2Box]
  implicit val decoder: JsonDecoder[Asset2Box] = DeriveJsonDecoder.gen[Asset2Box]
}

case class OutputRecords(
  byErgoTree: mutable.Map[ErgoTree, mutable.Set[Utxo]],
  byErgoTreeT8: mutable.Map[ErgoTreeT8, mutable.Set[Utxo]],
  utxosByTokenId: mutable.Map[TokenId, mutable.Set[BoxId]],
  tokensByUtxo: mutable.Map[BoxId, mutable.Map[TokenId, Amount]]
)

final case class Block(
  blockId: BlockId,
  parentId: BlockId,
  revision: Revision,
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
    Protocol.genesisBlockId,
    Protocol.genesisBlockId,
    0,
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

  def persistable(revision: Revision): Block = copy(revision = revision)
}

object Block {
  implicit val encoder: JsonEncoder[Block] = DeriveJsonEncoder.gen[Block]
  implicit val decoder: JsonDecoder[Block] = DeriveJsonDecoder.gen[Block]
}
