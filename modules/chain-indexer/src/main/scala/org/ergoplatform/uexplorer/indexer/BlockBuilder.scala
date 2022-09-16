package org.ergoplatform.uexplorer.indexer

import akka.NotUsed
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.scaladsl.Flow
import akka.util.Timeout
import cats.Applicative
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.BlockId
import org.ergoplatform.explorer.BuildFrom.syntax._
import org.ergoplatform.explorer.db.models.BlockStats
import org.ergoplatform.explorer.indexer.extractors._
import org.ergoplatform.explorer.indexer.models.{FlatBlock, SlotData}
import org.ergoplatform.explorer.protocol.models.ApiFullBlock
import org.ergoplatform.explorer.settings.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.api.BlockUpdater
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor._
import tofu.Context

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

class BlockBuilder(blockUpdater: BlockUpdater)(implicit s: ActorSystem[Nothing]) extends LazyLogging {

  implicit private val timeout: Timeout = 3.seconds

  protected def getBestBlockOrBranch(
    block: ApiFullBlock,
    blockHttpClient: BlockHttpClient,
    progressMonitor: ActorRef[ProgressMonitorRequest],
    acc: List[ApiFullBlock]
  ): Future[List[ApiFullBlock]] =
    progressMonitor
      .ask(ref => GetBlock(block.header.parentId, ref))
      .flatMap {
        case CachedBlock(Some(_)) =>
          Future.successful(block :: acc)
        case CachedBlock(None) if block.header.height == 1 =>
          Future.successful(block :: acc)
        case CachedBlock(None) =>
          logger.info(s"Encountered fork at height ${block.header.height} and block ${block.header.id}")
          blockHttpClient
            .getBlockForId(block.header.parentId)
            .flatMap(b => getBestBlockOrBranch(b, blockHttpClient, progressMonitor, block :: acc))
      }

  def blockBuildingFlow(
    blockHttpClient: BlockHttpClient,
    progressMonitor: ActorRef[ProgressMonitorRequest]
  ): Flow[ApiFullBlock, FlatBlock, NotUsed] =
    Flow[ApiFullBlock]
      .mapAsync(1) { block =>
        getBestBlockOrBranch(block, blockHttpClient, progressMonitor, List.empty)
          .flatMap {
            case bestBlock :: Nil =>
              progressMonitor.ask(ref => InsertBestBlock(bestBlock, ref))
            case winningFork =>
              progressMonitor.ask(ref => InsertWinningFork(winningFork, ref))
          }
      }
      .mapAsync(1) {
        case BestBlockInserted(flatBlock) =>
          Future.successful(List(flatBlock))
        case ForkInserted(newFlatBlocks, supersededFork) =>
          blockUpdater
            .removeBlocksFromMainChain(supersededFork.map(_.stats.headerId))
            .map(_ => newFlatBlocks)
      }
      .mapConcat(identity)
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
