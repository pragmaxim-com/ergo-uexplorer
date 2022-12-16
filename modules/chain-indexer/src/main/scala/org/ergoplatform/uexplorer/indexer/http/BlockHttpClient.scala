package org.ergoplatform.uexplorer.indexer.http

import akka.NotUsed
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
import org.ergoplatform.uexplorer.{BlockId, TxId}
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ApiTransaction}
import org.ergoplatform.uexplorer.indexer.config.{ChainIndexerConf, ProtocolSettings}
import org.ergoplatform.uexplorer.indexer.chain.ChainSyncer
import org.ergoplatform.uexplorer.indexer.chain.ChainSyncer.*
import org.ergoplatform.uexplorer.indexer.{Const, ResiliencySupport}
import retry.Policy
import sttp.capabilities.WebSockets
import sttp.client3.*
import sttp.client3.circe.*
import io.circe.refined.*
import org.ergoplatform.ErgoAddressEncoder

import scala.collection.immutable.{ArraySeq, ListMap}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class BlockHttpClient(metadataHttpClient: MetadataHttpClient[_])(implicit
  protocol: ProtocolSettings,
  s: ActorSystem[Nothing],
  chainSyncer: ActorRef[ChainSyncerRequest],
  sttpB: SttpBackend[Future, _]
) extends ResiliencySupport with Codecs {

  implicit private val addressEncoder: ErgoAddressEncoder = protocol.addressEncoder
  private val proxyUri                                    = uri"http://proxy"
  private val retryPolicy: Policy                         = retry.Backoff(3, 1.second)

  def getBestBlockHeight: Future[Int] =
    metadataHttpClient.getMasterNodes.map(_.minBy(_.fullHeight).fullHeight)

  def getUnconfirmedTxs: Future[ListMap[TxId, ApiTransaction]] =
    retryPolicy.apply { () =>
      basicRequest
        .get(proxyUri.addPath("transactions", "unconfirmed"))
        .response(asJson[List[ApiTransaction]])
        .readTimeout(5.seconds)
        .send(sttpB)
        .map(_.body)
        .flatMap {
          case Right(txs) =>
            Future.successful(ListMap.from(txs.iterator.map(tx => tx.id -> tx)))
          case Left(error) =>
            Future.failed(new Exception(s"Getting unconfirmed transactions failed", error))
        }
    }(retry.Success.always, global)

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
    ChainSyncer
      .containsBlock(block.header.parentId)
      .flatMap {
        case IsBlockCached(true) =>
          Future.successful(block :: acc)
        case IsBlockCached(false) if block.header.height == 1 =>
          Future.successful(block :: acc)
        case IsBlockCached(false) =>
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
              ChainSyncer.insertBestBlock(bestBlock)
            case winningFork =>
              ChainSyncer.insertWinningFork(winningFork)
          }
      }

  def close(): Future[Unit] =
    sttpB.close()

}

object BlockHttpClient {

  def withNodePoolBackend(
    conf: ChainIndexerConf
  )(implicit
    protocol: ProtocolSettings,
    ctx: ActorContext[_],
    chainSyncer: ActorRef[ChainSyncerRequest]
  ): Future[BlockHttpClient] = {
    val futureSttpBackend = HttpClientFutureBackend()
    val metadataClient    = MetadataHttpClient(conf)(futureSttpBackend, ctx.system)
    val nodePoolRef       = ctx.spawn(NodePool.behavior, "NodePool")
    val backend           = SttpNodePoolBackend[WebSockets](nodePoolRef)(ctx.system, futureSttpBackend)
    backend.keepNodePoolUpdated(metadataClient).map { _ =>
      new BlockHttpClient(metadataClient)(protocol, ctx.system, chainSyncer, backend)
    }
  }
}
