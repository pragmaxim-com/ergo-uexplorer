package org.ergoplatform.uexplorer.http

import akka.actor.CoordinatedShutdown
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.scaladsl.Flow
import akka.stream.{OverflowStrategy, SharedKillSwitch}
import akka.{Done, NotUsed}
import io.circe.refined.*
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.ExeContext.Implicits
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ApiTransaction}
import org.ergoplatform.uexplorer.{BlockId, Const, Height, ResiliencySupport, TxId}
import retry.Policy
import sttp.capabilities.WebSockets
import sttp.client3.*
import sttp.client3.circe.*

import scala.collection.immutable.{ArraySeq, ListMap}
import scala.collection.{concurrent, mutable}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

class BlockHttpClient(metadataHttpClient: MetadataHttpClient[_])(implicit
  sttpB: SttpBackend[Future, _]
) extends ResiliencySupport
  with Codecs {

  private val proxyUri            = uri"http://proxy"
  private val retryPolicy: Policy = retry.Backoff(3, 1.second)

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

  def getBlockIdsByOffset(fromHeightIncl: Int, limit: Int): Future[Vector[BlockId]] =
    retryPolicy.apply { () =>
      basicRequest
        .get(proxyUri.addPath("blocks").addParam("offset", fromHeightIncl.toString).addParam("limit", limit.toString))
        .response(asJson[Vector[BlockId]])
        .readTimeout(1.seconds)
        .send(sttpB)
        .map(_.body)
        .flatMap {
          case Right(blockIds) =>
            Future.successful(blockIds)
          case Left(error) =>
            Future.failed(new Exception(s"Getting block id at fromHeightIncl $fromHeightIncl failed", error))
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

  def getBlockForIdAsString(blockId: BlockId): Future[String] =
    retryPolicy.apply { () =>
      basicRequest
        .get(proxyUri.addPath("blocks", blockId.toString))
        .response(asString)
        .responseGetRight
        .readTimeout(5.seconds)
        .send(sttpB)
        .map(_.body)
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

  def close(): Future[Unit] = {
    logger.info(s"Stopping Block http client")
    sttpB.close()
  }

}

object BlockHttpClient {

  def withNodePoolBackend(implicit
    localNodeUriMagnet: LocalNodeUriMagnet,
    remoteNodeUriMagnet: RemoteNodeUriMagnet,
    ctx: ActorContext[_],
    killSwitch: SharedKillSwitch
  ): Future[BlockHttpClient] = {
    val futureSttpBackend = HttpClientFutureBackend()
    val metadataClient    = MetadataHttpClient(localNodeUriMagnet, remoteNodeUriMagnet, futureSttpBackend, ctx.system)
    val nodePoolRef       = ctx.spawn(NodePool.behavior, "NodePool")
    val backend           = SttpNodePoolBackend[WebSockets](nodePoolRef)(ctx.system, futureSttpBackend, killSwitch)
    backend.keepNodePoolUpdated(metadataClient).map { _ =>
      val blockClient = new BlockHttpClient(metadataClient)(backend)
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
