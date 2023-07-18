package org.ergoplatform.uexplorer.http

import io.circe.refined.*
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.ExeContext.Implicits
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ApiTransaction}
import org.ergoplatform.uexplorer.{BlockId, Const, Height, ResiliencySupport, TxId}
import sttp.client3.*
import sttp.client3.circe.*
import sttp.client3.httpclient.zio.HttpClientZioBackend
import zio.*

import scala.collection.immutable.{ArraySeq, ListMap}
import scala.collection.{concurrent, mutable}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import nl.vroste.rezilience.Retry
import nl.vroste.rezilience.Retry.Schedules
import sttp.capabilities.zio.ZioStreams

case class BlockHttpClient(metadataHttpClient: MetadataHttpClient, sttpB: SttpBackend[Task, ZioStreams]) extends Codecs {

  private val proxyUri = uri"http://proxy"

  def getBestBlockHeight: Task[Height] =
    metadataHttpClient.getMasterNodes.map(_.minBy(_.fullHeight).fullHeight)

  def getUnconfirmedTxs: Task[ListMap[TxId, ApiTransaction]] =
    basicRequest
      .get(proxyUri.addPath("transactions", "unconfirmed"))
      .response(asJson[List[ApiTransaction]])
      .readTimeout(5.seconds)
      .send(sttpB)
      .map(_.body)
      .flatMap {
        case Right(txs) =>
          ZIO.succeed(ListMap.from(txs.iterator.map(tx => tx.id -> tx)))
        case Left(error) =>
          ZIO.fail(new Exception(s"Getting unconfirmed transactions failed", error))
      }
      .retry(Schedules.common(1.second, 5.seconds, 2.0, true, Some(10)))

  def getBlockIdsByOffset(fromHeightIncl: Int, limit: Int): Task[Vector[BlockId]] =
    basicRequest
      .get(proxyUri.addPath("blocks").addParam("offset", fromHeightIncl.toString).addParam("limit", limit.toString))
      .response(asJson[Vector[BlockId]])
      .readTimeout(1.seconds)
      .send(sttpB)
      .map(_.body)
      .flatMap {
        case Right(blockIds) =>
          ZIO.succeed(blockIds)
        case Left(error) =>
          ZIO.fail(new Exception(s"Getting block id at fromHeightIncl $fromHeightIncl failed", error))
      }
      .retry(Schedule.exponential(1.seconds, 2.0).upTo(1.minute))

  def getBlockIdForHeight(height: Int): Task[BlockId] =
    basicRequest
      .get(proxyUri.addPath("blocks", "at", height.toString))
      .response(asJson[List[BlockId]])
      .readTimeout(1.seconds)
      .send(sttpB)
      .map(_.body)
      .flatMap {
        case Right(blockIds) if blockIds.nonEmpty =>
          ZIO.succeed(blockIds.head)
        case Right(_) =>
          ZIO.fail(new Exception(s"There is no block at height $height"))
        case Left(error) =>
          ZIO.fail(new Exception(s"Getting block id at height $height failed", error))
      }
      .retry(Schedules.common(1.second, 5.seconds, 2.0, true, Some(10)))

  def getBlockForIdAsString(blockId: BlockId): Task[String] =
    basicRequest
      .get(proxyUri.addPath("blocks", blockId.toString))
      .response(asString)
      .responseGetRight
      .readTimeout(5.seconds)
      .send(sttpB)
      .map(_.body)
      .retry(Schedules.common(1.second, 5.seconds, 2.0, true, Some(10)))

  def getBlockForId(blockId: BlockId): Task[ApiFullBlock] =
    basicRequest
      .get(proxyUri.addPath("blocks", blockId.toString))
      .response(asJson[ApiFullBlock])
      .responseGetRight
      .readTimeout(5.seconds)
      .send(sttpB)
      .map(_.body)
      .retry(Schedules.common(1.second, 5.seconds, 2.0, true, Some(10)))

  def close(): Task[Unit] =
    sttpB.close()

}

object BlockHttpClient {

  def layer: ZLayer[MetadataHttpClient with SttpBackend[Task, ZioStreams], Nothing, BlockHttpClient] =
    ZLayer.fromFunction(BlockHttpClient.apply _)

}
