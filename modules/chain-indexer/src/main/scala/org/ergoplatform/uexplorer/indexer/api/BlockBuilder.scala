package org.ergoplatform.uexplorer.indexer.api

import akka.NotUsed
import akka.actor.typed.ActorRef
import akka.stream.scaladsl.Flow
import cats.Applicative
import org.ergoplatform.explorer.BlockId
import org.ergoplatform.explorer.BuildFrom.syntax._
import org.ergoplatform.explorer.db.models.BlockStats
import org.ergoplatform.explorer.indexer.extractors._
import org.ergoplatform.explorer.indexer.models.{FlatBlock, SlotData}
import org.ergoplatform.explorer.protocol.models.ApiFullBlock
import org.ergoplatform.explorer.settings.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.StopException
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor.ProgressMonitorRequest
import tofu.Context

import scala.util.{Failure, Success, Try}

trait BlockBuilder {

  def blockBuildingFlow(
    blockHttpClient: BlockHttpClient,
    progressMonitor: ActorRef[ProgressMonitorRequest]
  ): Flow[ApiFullBlock, FlatBlock, NotUsed]
}

object BlockBuilder {

  case class BlockInfo(parentId: BlockId, stats: BlockStats)

  def updateMainChain(block: FlatBlock, mainChain: Boolean): FlatBlock = {
    import monocle.macros.syntax.lens._
    block
      .lens(_.header.mainChain)
      .modify(_ => mainChain)
      .lens(_.info.mainChain)
      .modify(_ => mainChain)
      .lens(_.txs)
      .modify(_.map(_.copy(mainChain = mainChain)))
      .lens(_.inputs)
      .modify(_.map(_.copy(mainChain = mainChain)))
      .lens(_.dataInputs)
      .modify(_.map(_.copy(mainChain = mainChain)))
      .lens(_.outputs)
      .modify(_.map(_.copy(mainChain = mainChain)))
  }

  def buildBlock(apiBlock: ApiFullBlock, prevBlockInfo: Option[BlockStats])(implicit
    protocol: ProtocolSettings
  ): FlatBlock = {
    implicit val ctx = Context.const(protocol)(Applicative[Try])
    SlotData(apiBlock, prevBlockInfo)
      .intoF[Try, FlatBlock]
      .map(updateMainChain(_, mainChain = true)) match {
      case Success(b) =>
        b
      case Failure(ex) =>
        throw new StopException(s"Block $apiBlock with prevBlockInfo $prevBlockInfo is invalid", ex)
    }
  }

  implicit class FlatBlockPimp(underlying: FlatBlock) {
    def buildInfo: BlockInfo = BlockInfo(underlying.header.parentId, underlying.info)
  }
}
