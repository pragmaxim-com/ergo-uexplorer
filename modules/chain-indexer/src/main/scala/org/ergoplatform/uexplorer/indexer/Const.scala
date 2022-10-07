package org.ergoplatform.uexplorer.indexer

import org.ergoplatform.ErgoScriptPredef
import scorex.util.encode.Base16

object Const {

  val CassandraKeyspace = "uexplorer"
  val EpochLength       = 1024
  val BufferSize        = 32
  val FlushHeight       = 32
  val AllowedHeightDiff = 10
  val MinNodeHeight     = EpochLength * 800

  val FeeContractAddress =
    "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe"

  val MinerRewardDelta = 720

  val TeamTreasuryThreshold = 67500000000L

  val FeePropositionScriptHex: String = {
    val script = ErgoScriptPredef.feeProposition(MinerRewardDelta)
    Base16.encode(script.bytes)
  }

  val CoinsInOneErgo: Long = 1000000000L

  val Eip27UpperPoint        = 15 * CoinsInOneErgo
  val Eip27DefaultReEmission = 12 * CoinsInOneErgo
  val Eip27LowerPoint        = 3 * CoinsInOneErgo
  val Eip27ResidualEmission  = 3 * CoinsInOneErgo

  val MainnetEip27ActivationHeight = 777217
  val TestnetEip27ActivationHeight = 188001

}
