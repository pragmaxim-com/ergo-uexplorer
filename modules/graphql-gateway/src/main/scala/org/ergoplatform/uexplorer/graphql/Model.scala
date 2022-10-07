package org.ergoplatform.uexplorer.graphql

import java.time.Instant
import java.util.{Currency, UUID}

case class BlockInfo(
  blockSize: Int,
  blockCoins: Long,
  blockMiningTime: Long,
  txsCount: Int,
  txsSize: Int,
  minerAddress: String,
  minerReward: Long,
  minerRevenue: Long,
  blockFee: Long,
  blockChainTotalSize: Long,
  totalTxsCount: Long,
  totalCoinsIssued: Long,
  totalMiningTime: Long,
  totalFees: Long,
  totalMinersReward: Long,
  totalCoinsInTxs: Long,
  maxTxGix: Long,
  maxBoxGix: Long
)

case class Header(
  headerId: String,
  parentId: String,
  height: Int,
  timestamp: Long,
  difficulty: BigDecimal,
  version: Byte,
  nBits: Long,
  stateRoot: String,
  adProofsRoot: String,
  adProofsBytes: String,
  adProofsDigest: String,
  extensionsDigest: String,
  extensionsFields: String,
  transactionsRoot: String,
  extensionHash: String,
  minerPk: String,
  w: String,
  n: String,
  d: String,
  votes: String,
  mainChain: Boolean,
  blockInfo: BlockInfo
)
