package org.ergoplatform.uexplorer.indexer

import org.ergoplatform.uexplorer.Const.{EpochLength, MinerRewardDelta}
import org.ergoplatform.uexplorer.HexString
import org.ergoplatform.ErgoScriptPredef
import scorex.util.encode.Base16

object Const {
  val CassandraKeyspace = "uexplorer"
  val AllowedHeightDiff = 10
  val MinNodeHeight     = EpochLength * 800
  val FlushHeight       = 32

  val FeePropositionScriptHex: HexString = {
    val script = ErgoScriptPredef.feeProposition(MinerRewardDelta)
    HexString.fromStringUnsafe(Base16.encode(script.bytes))
  }

}
