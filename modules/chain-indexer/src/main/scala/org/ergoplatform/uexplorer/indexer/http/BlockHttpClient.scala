package org.ergoplatform.uexplorer.indexer.http

import akka.NotUsed
import akka.actor.typed.scaladsl.ActorContext
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
import org.ergoplatform.explorer.BlockId
import org.ergoplatform.explorer.protocol.models.ApiFullBlock
import org.ergoplatform.uexplorer.indexer.{ResiliencySupport, StopException}
import retry.Policy
import sttp.client3._
import sttp.client3.circe._
import sttp.model.Uri

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class BlockHttpClient(implicit val sttpB: SttpBackend[Future, _]) extends ResiliencySupport {

  private val proxyUri            = uri"http://proxy"
  private val retryPolicy: Policy = retry.Backoff(3, 1.second)

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
            Future.failed(new StopException(s"There is no block at height $height", null))
          case Left(error) =>
            Future.failed(new StopException(s"Getting block id at height $height failed", error))
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

  def blockResolvingFlow: Flow[Int, ApiFullBlock, NotUsed] =
    Flow[Int]
      .mapAsync(1)(getBlockIdForHeight)
      .buffer(64, OverflowStrategy.backpressure)
      .mapAsync(1)(getBlockForId)
      .buffer(32, OverflowStrategy.backpressure)

  def close(): Future[Unit] =
    sttpB.close()

}

object BlockHttpClient {

  def apply(metadataClient: MetadataHttpClient[_])(implicit ctx: ActorContext[_]): BlockHttpClient = {
    val nodePoolRef = ctx.spawn(NodePool.behavior(metadataClient), "NodePool")
    new BlockHttpClient()(NodePoolSttpBackendWrapper(nodePoolRef, metadataClient)(ctx.system))
  }

}
