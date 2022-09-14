package org.ergoplatform.uexplorer.indexer.http

import akka.NotUsed
import akka.actor.typed.scaladsl.ActorContext
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.BlockId
import org.ergoplatform.explorer.protocol.models.ApiFullBlock
import org.ergoplatform.uexplorer.indexer.Resiliency
import sttp.client3._
import sttp.client3.circe._
import sttp.model.Uri

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class BlockHttpClient(metadataClient: MetadataHttpClient[_], masterPeerAddresses: List[Uri])(implicit
  val sttpB: SttpBackend[Future, _]
) extends LazyLogging {

  def getBestBlockHeight: Future[Int] = metadataClient.getMasterInfo.map(_.fullHeight.getOrElse(0))

  def getBlockIdForHeight(height: Int): Future[BlockId] =
    Resiliency.fallback(masterPeerAddresses, retry.Backoff(3, 1.second)) { uri =>
      basicRequest
        .get(uri.addPath("blocks", "at", height.toString))
        .response(asJson[List[BlockId]])
        .readTimeout(5.seconds)
        .send(sttpB)
        .map(_.body)
        .flatMap {
          case Right(blockIds) if blockIds.nonEmpty =>
            Future.successful(blockIds.head)
          case Right(_) =>
            Future.failed(new Resiliency.StopException(s"There is no block at height $height", null))
          case Left(error) =>
            Future.failed(new Resiliency.StopException(s"Getting block id at height $height failed", error))

        }
    }

  def getBlockForId(blockId: BlockId): Future[ApiFullBlock] =
    Resiliency.fallback(masterPeerAddresses, retry.Backoff(3, 1.second)) { uri =>
      basicRequest
        .get(uri.addPath("blocks", blockId.toString))
        .response(asJson[ApiFullBlock])
        .readTimeout(5.seconds)
        .send(sttpB)
        .map(_.body)
        .flatMap {
          case Right(block) =>
            Future.successful(block)
          case Left(error) =>
            Future.failed(new Resiliency.StopException(s"Getting block id $blockId failed due to $error", null))
        }
    }

  def blockResolvingFlow: Flow[Int, ApiFullBlock, NotUsed] =
    Flow[Int]
      .mapAsync(1)(getBlockIdForHeight)
      .buffer(64, OverflowStrategy.backpressure)
      .mapAsync(1)(getBlockForId)
      .buffer(32, OverflowStrategy.backpressure)

  def close(): Future[Unit] =
    metadataClient.underlyingB.close().andThen { case _ =>
      sttpB.close()
    }

}

object BlockHttpClient {

  def remote(masterPeerAddress: Uri, localNodeAddress: Uri, metadataClient: MetadataHttpClient[_])(implicit
    ctx: ActorContext[_]
  ): BlockHttpClient = {
    val nodePoolRef = ctx.spawn(NodePool.initialBehavior(metadataClient), "NodePool")
    new BlockHttpClient(metadataClient, List(masterPeerAddress, localNodeAddress))(
      NodePoolSttpBackendWrapper(nodePoolRef, metadataClient)(ctx.system)
    )
  }

  def local(localNodeAddress: Uri, metadataClient: MetadataHttpClient[_])(implicit
    sttpB: SttpBackend[Future, _]
  ): BlockHttpClient =
    new BlockHttpClient(metadataClient, List(localNodeAddress))(sttpB)

}
