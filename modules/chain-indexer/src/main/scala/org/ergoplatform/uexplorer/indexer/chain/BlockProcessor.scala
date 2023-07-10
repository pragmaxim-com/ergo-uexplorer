package org.ergoplatform.uexplorer.indexer.chain

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.{Attributes, OverflowStrategy}
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.ProtocolSettings
import org.ergoplatform.uexplorer.chain.ChainLinker
import org.ergoplatform.uexplorer.db.{BlockWithOutputs, LinkedBlock, OutputBuilder, RewardCalculator}
import org.ergoplatform.uexplorer.node.ApiFullBlock

object BlockProcessor {

  def processingFlow(
    chainLinker: ChainLinker
  )(implicit ps: ProtocolSettings): Flow[ApiFullBlock, List[LinkedBlock], NotUsed] =
    Flow[ApiFullBlock]
      .map(RewardCalculator(_).get)
      .async
      .buffer(4096, OverflowStrategy.backpressure)
      .map(OutputBuilder(_)(ps.addressEncoder).get)
      .async
      .addAttributes(Attributes.inputBuffer(1, 8)) // contract processing (sigma, base58)
      .buffer(4096, OverflowStrategy.backpressure)
      .mapAsync(1)(chainLinker.linkChildToAncestors())
      .buffer(4096, OverflowStrategy.backpressure)

}
