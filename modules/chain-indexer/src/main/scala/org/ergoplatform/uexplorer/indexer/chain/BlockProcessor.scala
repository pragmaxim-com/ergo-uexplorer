package org.ergoplatform.uexplorer.indexer.chain

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.ProtocolSettings
import org.ergoplatform.uexplorer.chain.ChainLinker
import org.ergoplatform.uexplorer.db.{BlockWithOutputs, LinkedBlock, OutputBuilder, RewardCalculator}
import org.ergoplatform.uexplorer.node.ApiFullBlock
import zio.*
import zio.stream.ZPipeline
import org.ergoplatform.uexplorer.db.BlockWithReward

object BlockProcessor {

  def processingFlow(
    chainLinker: ChainLinker
  )(implicit ps: ProtocolSettings) =
    ZPipeline
      .mapZIOPar[Any, Throwable, ApiFullBlock, BlockWithReward](2)(b => ZIO.fromTry(RewardCalculator(b)))
      .mapZIOPar(2)(b => ZIO.fromTry(OutputBuilder(b)(ps.addressEncoder)))
      .mapZIO(b => chainLinker.linkChildToAncestors()(b))

}
