package org.ergoplatform.uexplorer.db

import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.Const.Protocol
import org.ergoplatform.uexplorer.Const.Protocol.Emission
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.HexString.unwrapped
import org.ergoplatform.uexplorer.Address.unwrappedAddress
import org.ergoplatform.uexplorer.Const.Protocol
import org.ergoplatform.uexplorer.Const.Protocol.Emission

import scala.collection.mutable

trait BoxLike {
  def boxId: BoxId
  def blockId: BlockId
  def txId: TxId
  def ergoTreeHash: ErgoTreeHash
  def ergoTreeT8Hash: Option[ErgoTreeT8Hash]
  def ergValue: Value
}

case class Box(
  boxId: BoxId,
  blockId: BlockId,
  txId: TxId,
  ergoTreeHash: ErgoTreeHash,
  ergoTreeT8Hash: Option[ErgoTreeT8Hash],
  ergValue: Value
) extends BoxLike

case class Utxo(
  boxId: BoxId,
  blockId: BlockId,
  txId: TxId,
  ergoTreeHash: ErgoTreeHash,
  ergoTreeT8Hash: Option[ErgoTreeT8Hash],
  ergValue: Value
) extends BoxLike {
  def toBox = Box(boxId, blockId, txId, ergoTreeHash, ergoTreeT8Hash, ergValue)
}

case class ErgoTree(hash: ErgoTreeHash, blockId: BlockId, hex: ErgoTreeHex)
case class ErgoTreeT8(hash: ErgoTreeHash, blockId: BlockId, hex: ErgoTreeHex)

case class OutputRecords(
  byErgoTree: mutable.Map[ErgoTree, mutable.Set[Utxo]],
  byErgoTreeT8: mutable.Map[ErgoTreeT8, mutable.Set[Utxo]]
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
