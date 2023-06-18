package org.ergoplatform.uexplorer.http

import akka.{Done, NotUsed}
import akka.actor.CoordinatedShutdown
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.{OverflowStrategy, SharedKillSwitch}
import akka.stream.scaladsl.Flow
import org.ergoplatform.uexplorer.{BlockId, Const, Height, TxId}
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ApiTransaction}
import retry.Policy
import sttp.capabilities.WebSockets
import sttp.client3.*
import sttp.client3.circe.*
import io.circe.refined.*
import org.ergoplatform.ErgoAddressEncoder

import scala.collection.immutable.{ArraySeq, ListMap}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import org.ergoplatform.uexplorer.ProtocolSettings
import org.ergoplatform.uexplorer.ResiliencySupport
import org.ergoplatform.uexplorer.ExeContext.Implicits

class BlockHttpClient(metadataHttpClient: MetadataHttpClient[_])(implicit
  protocol: ProtocolSettings,
  sttpB: SttpBackend[Future, _]
) extends ResiliencySupport
  with Codecs {

  implicit private val addressEncoder: ErgoAddressEncoder = protocol.addressEncoder
  private val proxyUri                                    = uri"http://proxy"
  private val retryPolicy: Policy                         = retry.Backoff(3, 1.second)

  def getBestBlockHeight: Future[Height] =
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
        .readTimeout(5.seconds)
        .send(sttpB)
        .map(_.body)
    }(retry.Success.always, global)

  def getBestBlockOrBranch(
    block: ApiFullBlock,
    isBlockCached: (BlockId, Height) => Boolean,
    acc: List[ApiFullBlock]
  )(implicit ec: ExecutionContext): Future[List[ApiFullBlock]] =
    isBlockCached(block.header.parentId, block.header.height - 1) match {
      case blockCached if blockCached =>
        Future.successful(block :: acc)
      case _ if block.header.height == 1 =>
        Future.successful(block :: acc)
      case _ =>
        logger.info(s"Encountered fork at height ${block.header.height} and block ${block.header.id}")
        getBlockForId(block.header.parentId)
          .flatMap(b => getBestBlockOrBranch(b, isBlockCached, block :: acc))
    }

  def blockFlow: Flow[Height, ApiFullBlock, NotUsed] =
    Flow[Height]
      .mapAsync(1)(getBlockIdForHeight)
      .buffer(512, OverflowStrategy.backpressure)
      .mapAsync(1)(getBlockForId) // parallelism could be parameterized - low or big pressure on Node
      .buffer(512, OverflowStrategy.backpressure)

  def close(): Future[Unit] = {
    logger.info(s"Stopping Block http client")
    sttpB.close()
  }

}

object BlockHttpClient {

  def withNodePoolBackend(implicit
    localNodeUriMagnet: LocalNodeUriMagnet,
    remoteNodeUriMagnet: RemoteNodeUriMagnet,
    protocol: ProtocolSettings,
    ctx: ActorContext[_],
    killSwitch: SharedKillSwitch
  ): Future[BlockHttpClient] = {
    val futureSttpBackend = HttpClientFutureBackend()
    val metadataClient    = MetadataHttpClient(localNodeUriMagnet, remoteNodeUriMagnet, futureSttpBackend, ctx.system)
    val nodePoolRef       = ctx.spawn(NodePool.behavior, "NodePool")
    val backend           = SttpNodePoolBackend[WebSockets](nodePoolRef)(ctx.system, futureSttpBackend, killSwitch)
    backend.keepNodePoolUpdated(metadataClient).map { _ =>
      val blockClient = new BlockHttpClient(metadataClient)(protocol, backend)
      CoordinatedShutdown(ctx.system).addTask(
        CoordinatedShutdown.PhaseServiceStop,
        "stop-block-http-client"
      ) { () =>
        blockClient.close().map(_ => Done)
      }
      blockClient
    }
  }
}
