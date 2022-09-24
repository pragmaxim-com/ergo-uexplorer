package org.ergoplatform.uexplorer.indexer.http

import akka.NotUsed
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
import cats.Applicative
import org.ergoplatform.explorer.BlockId
import org.ergoplatform.explorer.BuildFrom.syntax._
import org.ergoplatform.explorer.db.models.BlockStats
import org.ergoplatform.explorer.indexer.extractors._
import org.ergoplatform.explorer.indexer.models.{FlatBlock, SlotData}
import org.ergoplatform.explorer.protocol.models.ApiFullBlock
import org.ergoplatform.explorer.settings.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor._
import org.ergoplatform.uexplorer.indexer.{Const, ResiliencySupport}
import retry.Policy
import sttp.capabilities.WebSockets
import sttp.client3._
import sttp.client3.circe._
import tofu.Context

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Try

class BlockHttpClient(metadataHttpClient: MetadataHttpClient[_])(implicit
  s: ActorSystem[Nothing],
  progressMonitor: ActorRef[MonitorRequest],
  sttpB: SttpBackend[Future, _]
) extends ResiliencySupport {

  private val proxyUri            = uri"http://proxy"
  private val retryPolicy: Policy = retry.Backoff(3, 1.second)

  def getBestBlockHeight: Future[Int] = metadataHttpClient.getMasterNodes.map(_.minBy(_.fullHeight).fullHeight)

  def getBlockIdForHeight(height: Int): Future[BlockId] =
    retryPolicy.apply { () =>
      basicRequest
        .get(proxyUri.addPath("blocks", "at", height.toString))
        .response(asJson[List[BlockId]])
        .readTimeout(1.seconds)
        .send(sttpB)
        .map(_.body)
        .flatMap {
          case Right(blockIds) if blockIds.nonEmpty =>
            Future.successful(blockIds.head)
          case Right(_) =>
            Future.failed(new Exception(s"There is no block at height $height"))
          case Left(error) =>
            Future.failed(new Exception(s"Getting block id at height $height failed", error))
        }
    }(retry.Success.always, global)

  def getBlockForId(blockId: BlockId): Future[ApiFullBlock] =
    retryPolicy.apply { () =>
      basicRequest
        .get(proxyUri.addPath("blocks", blockId.toString))
        .response(asJson[ApiFullBlock])
        .responseGetRight
        .readTimeout(3.seconds)
        .send(sttpB)
        .map(_.body)
    }(retry.Success.always, global)

  def getBestBlockOrBranch(
    block: ApiFullBlock,
    acc: List[ApiFullBlock]
  ): Future[List[ApiFullBlock]] =
    ProgressMonitor
      .getBlock(block.header.parentId)
      .flatMap {
        case CachedBlock(Some(_)) =>
          Future.successful(block :: acc)
        case CachedBlock(None) if block.header.height == 1 =>
          Future.successful(block :: acc)
        case CachedBlock(None) =>
          logger.info(s"Encountered fork at height ${block.header.height} and block ${block.header.id}")
          getBlockForId(block.header.parentId)
            .flatMap(b => getBestBlockOrBranch(b, block :: acc))
      }

  def blockCachingFlow: Flow[Int, Inserted, NotUsed] =
    Flow[Int]
      .mapAsync(1)(getBlockIdForHeight)
      .buffer(Const.BufferSize * 2, OverflowStrategy.backpressure)
      .mapAsync(1)(getBlockForId)
      .buffer(Const.BufferSize, OverflowStrategy.backpressure)
      .mapAsync(1) { block =>
        getBestBlockOrBranch(block, List.empty)
          .flatMap {
            case bestBlock :: Nil =>
              ProgressMonitor.insertBestBlock(bestBlock)
            case winningFork =>
              ProgressMonitor.insertWinningFork(winningFork)
          }
      }

  def close(): Future[Unit] =
    sttpB.close()

}

object BlockHttpClient {

  def withNodePoolBackend(
    conf: ChainIndexerConf
  )(implicit ctx: ActorContext[_], progressMonitor: ActorRef[MonitorRequest]): Future[BlockHttpClient] = {
    val futureSttpBackend = HttpClientFutureBackend()
    val metadataClient    = MetadataHttpClient(conf)(futureSttpBackend, ctx.system)
    val nodePoolRef       = ctx.spawn(NodePool.behavior, "NodePool")
    val backend           = SttpNodePoolBackend[WebSockets](nodePoolRef)(ctx.system, futureSttpBackend)
    backend.keepNodePoolUpdated(metadataClient).map { _ =>
      new BlockHttpClient(metadataClient)(ctx.system, progressMonitor, backend)
    }
  }

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
  ): Try[FlatBlock] = {
    implicit val ctx = Context.const(protocol)(Applicative[Try])
    SlotData(apiBlock, prevBlockInfo)
      .intoF[Try, FlatBlock]
      .map(updateMainChain(_, mainChain = true))
  }

  implicit class FlatBlockPimp(underlying: FlatBlock) {
    def buildInfo: BlockInfo = BlockInfo(underlying.header.parentId, underlying.info)
  }

}
