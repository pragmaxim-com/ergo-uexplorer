package org.ergoplatform.uexplorer.indexer.api

import akka.NotUsed
import akka.actor.typed.ActorRef
import akka.stream.scaladsl.Flow
import cats.Applicative
import org.ergoplatform.explorer.BuildFrom.syntax._
import org.ergoplatform.explorer.db.models.{BlockStats, Header}
import org.ergoplatform.explorer.indexer.extractors._
import org.ergoplatform.explorer.indexer.models.{FlatBlock, SlotData}
import org.ergoplatform.explorer.protocol.models.ApiFullBlock
import org.ergoplatform.explorer.settings.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.Resiliency.StopException
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor.ProgressMonitorRequest
import tofu.Context

import scala.util.Try

trait BlockBuilder {

  def blockBuildingFlow(
    blockHttpClient: BlockHttpClient,
    progressMonitor: ActorRef[ProgressMonitorRequest]
  ): Flow[ApiFullBlock, FlatBlock, NotUsed]
}

object BlockBuilder {

  def updateMainChain(block: FlatBlock, mainChain: Boolean): FlatBlock = {
    import monocle.macros.GenLens
    val header          = GenLens[FlatBlock](_.header)
    val headerMainChain = GenLens[Header](_.mainChain)
    val info            = GenLens[FlatBlock](_.info)
    val infoMainChain   = GenLens[BlockStats](_.mainChain)
    val txs             = GenLens[FlatBlock](_.txs)
    val inputs          = GenLens[FlatBlock](_.inputs)
    val dataInputs      = GenLens[FlatBlock](_.dataInputs)
    val outputs         = GenLens[FlatBlock](_.outputs)

    (header composeLens headerMainChain).modify(_ => mainChain)(
      (info composeLens infoMainChain).modify(_ => mainChain)(
        txs.modify(_.map(_.copy(mainChain = mainChain)))(
          inputs.modify(_.map(_.copy(mainChain = mainChain)))(
            dataInputs.modify(_.map(_.copy(mainChain = mainChain)))(
              outputs.modify(_.map(_.copy(mainChain = mainChain)))(
                block
              )
            )
          )
        )
      )
    )
  }

  def buildBlock(slotData: SlotData, protocol: ProtocolSettings): FlatBlock = {
    implicit val ctx = Context.const(protocol)(Applicative[Try])
    slotData
      .intoF[Try, FlatBlock]
      .map(updateMainChain(_, mainChain = true))
      .getOrElse(throw new StopException(s"Block $slotData is invalid", null))
  }

}
