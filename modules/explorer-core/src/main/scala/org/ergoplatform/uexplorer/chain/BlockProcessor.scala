package org.ergoplatform.uexplorer.chain

import org.ergoplatform.uexplorer.CoreConf
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.node.ApiFullBlock
import zio.*
import zio.stream.ZPipeline

object BlockProcessor {

  def processingFlow(
    chainLinker: ChainLinker
  )(implicit ps: CoreConf): ZPipeline[Any, Throwable, ApiFullBlock, List[LinkedBlock]] =
    ZPipeline
      .mapZIO[Any, Throwable, ApiFullBlock, BlockWithReward](b => RewardCalculator(b))
      .mapZIO(b => OutputBuilder(b)(ps.addressEncoder))
      .mapZIO(b => chainLinker.linkChildToAncestors()(b))

}
