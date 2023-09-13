package org.ergoplatform.uexplorer.db

import eu.timepit.refined.auto.*
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.Const.Protocol
import org.ergoplatform.uexplorer.Const.Protocol.Emission
import org.ergoplatform.uexplorer.RegisterId.*
import org.ergoplatform.uexplorer.node.ExpandedRegister
import org.ergoplatform.uexplorer.parser.{ErgoTreeParser, RegistersParser}
import zio.*
import zio.json.*
import zio.json.interop.refined.*

import scala.collection.mutable

case class BoxRegister(
  serializedValue: BoxRegisterValueHex,
  sigmaType: Option[SigmaType],
  renderedValue: Option[String]
)

object BoxRegister {
  given JsonEncoder[SigmaType]   = JsonEncoder.string.contramap(s => SigmaType.encoder(s).noSpaces.trim)
  given JsonEncoder[BoxRegister] = DeriveJsonEncoder.gen[BoxRegister]

  given JsonDecoder[Option[SigmaType]] = JsonDecoder.string.map(s => SigmaType.parse(s))
  given JsonDecoder[BoxRegister]       = DeriveJsonDecoder.gen[BoxRegister]
}

case class BoxWithAssets(
  boxId: BoxId,
  transactionId: TxId,
  blockId: BlockId,
  value: Value,
  index: Index,
  globalIndex: GlobalIndex,
  creationHeight: Height,
  settlementHeight: Height,
  ergoTree: ErgoTreeHex,
  address: Address,
  assets: Iterable[Asset2Box],
  additionalRegisters: List[(Reg, BoxRegister)]
)

object BoxWithAssets {
  given JsonFieldEncoder[Reg]      = JsonFieldEncoder.string.contramap[Reg](_.unwrapped)
  given JsonEncoder[BoxWithAssets] = DeriveJsonEncoder.gen[BoxWithAssets]

  given JsonFieldDecoder[Reg]      = JsonFieldDecoder.string.map(r => Reg(RegisterId.valueOf(r)))
  given JsonDecoder[BoxWithAssets] = DeriveJsonDecoder.gen[BoxWithAssets]

  def fromBox(assetsByBox: List[(((Boxish, ErgoTree), Block), Iterable[Asset2Box])])(implicit enc: ErgoAddressEncoder): Task[List[BoxWithAssets]] =
    ZIO.foreachPar(assetsByBox) { case (((box, ergoTree), block), assets2box) =>
      ErgoTreeParser
        .ergoTreeHex2Base58Address(ergoTree.hex)
        .map { address =>
          BoxWithAssets(
            box.boxId,
            box.txId,
            block.blockId,
            box.ergValue,
            box.index,
            block.maxBoxGix,
            box.creationHeight,
            box.settlementHeight,
            ergoTree.hex,
            address,
            assets2box,
            buildRegisters(box).toList
          )
        }
    }

  def buildRegisters(box: Boxish): Map[Reg, BoxRegister] = {
    def expandedRegToBoxReg(r: ExpandedRegister) =
      BoxRegister(r.serializedValue, r.regValue.map(_.sigmaType), r.regValue.map(_.value))
    List(
      box.r4.map(r => Reg(R4) -> expandedRegToBoxReg(RegistersParser.parseAny(r))),
      box.r5.map(r => Reg(R5) -> expandedRegToBoxReg(RegistersParser.parseAny(r))),
      box.r6.map(r => Reg(R6) -> expandedRegToBoxReg(RegistersParser.parseAny(r))),
      box.r7.map(r => Reg(R7) -> expandedRegToBoxReg(RegistersParser.parseAny(r))),
      box.r8.map(r => Reg(R8) -> expandedRegToBoxReg(RegistersParser.parseAny(r))),
      box.r9.map(r => Reg(R9) -> expandedRegToBoxReg(RegistersParser.parseAny(r)))
    ).flatten.toMap
  }

}

sealed trait Boxish {
  def boxId: BoxId
  def txId: TxId
  def blockId: BlockId
  def creationHeight: CreationHeight
  def settlementHeight: SettlementHeight
  def ergoTreeHash: ErgoTreeHash
  def ergoTreeT8Hash: Option[ErgoTreeT8Hash]
  def ergValue: Value
  def index: Index
  def r4: Option[BoxRegisterValueHex]
  def r5: Option[BoxRegisterValueHex]
  def r6: Option[BoxRegisterValueHex]
  def r7: Option[BoxRegisterValueHex]
  def r8: Option[BoxRegisterValueHex]
  def r9: Option[BoxRegisterValueHex]
}

case class Box(
  boxId: BoxId,
  txId: TxId,
  blockId: BlockId,
  creationHeight: CreationHeight,
  settlementHeight: SettlementHeight,
  ergoTreeHash: ErgoTreeHash,
  ergoTreeT8Hash: Option[ErgoTreeT8Hash],
  ergValue: Value,
  index: Index,
  r4: Option[BoxRegisterValueHex],
  r5: Option[BoxRegisterValueHex],
  r6: Option[BoxRegisterValueHex],
  r7: Option[BoxRegisterValueHex],
  r8: Option[BoxRegisterValueHex],
  r9: Option[BoxRegisterValueHex]
) extends Boxish

object Box {
  implicit val encoder: JsonEncoder[Box] = DeriveJsonEncoder.gen[Box]
  implicit val decoder: JsonDecoder[Box] = DeriveJsonDecoder.gen[Box]
}

case class Utxo(
  boxId: BoxId,
  txId: TxId,
  blockId: BlockId,
  creationHeight: CreationHeight,
  settlementHeight: SettlementHeight,
  ergoTreeHash: ErgoTreeHash,
  ergoTreeT8Hash: Option[ErgoTreeT8Hash],
  ergValue: Value,
  index: Index,
  r4: Option[BoxRegisterValueHex],
  r5: Option[BoxRegisterValueHex],
  r6: Option[BoxRegisterValueHex],
  r7: Option[BoxRegisterValueHex],
  r8: Option[BoxRegisterValueHex],
  r9: Option[BoxRegisterValueHex]
) extends Boxish {
  def toBox: Box =
    Box(
      boxId,
      txId,
      blockId,
      creationHeight,
      settlementHeight,
      ergoTreeHash,
      ergoTreeT8Hash,
      ergValue,
      index,
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

final case class Token(
  index: Index,
  amount: Amount,
  name: Option[String],
  description: Option[String],
  `type`: Option[TokenType],
  decimals: Option[Int]
)

case class Asset(tokenId: TokenId, blockId: BlockId)
object Asset {
  implicit val encoder: JsonEncoder[Asset] = DeriveJsonEncoder.gen[Asset]
  implicit val decoder: JsonDecoder[Asset] = DeriveJsonDecoder.gen[Asset]
}

case class Asset2Box(
  tokenId: TokenId,
  boxId: BoxId,
  index: Index,
  amount: Amount,
  name: Option[String],
  description: Option[String],
  `type`: Option[TokenType],
  decimals: Option[Int]
)
object Asset2Box {
  given JsonEncoder[TokenType] = JsonEncoder[String].contramap(_.unwrapped)
  given JsonDecoder[TokenType] = JsonDecoder[String].map(TokenType.castUnsafe)
  given JsonEncoder[Asset2Box] = DeriveJsonEncoder.gen[Asset2Box]
  given JsonDecoder[Asset2Box] = DeriveJsonDecoder.gen[Asset2Box]
}

case class OutputRecords(
  byErgoTree: mutable.Map[ErgoTree, mutable.Set[Utxo]],
  byErgoTreeT8: mutable.Map[ErgoTreeT8, mutable.Set[Utxo]],
  utxosByTokenId: mutable.Map[TokenId, mutable.Set[BoxId]],
  tokensByUtxo: mutable.Map[BoxId, mutable.Map[TokenId, Token]]
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
