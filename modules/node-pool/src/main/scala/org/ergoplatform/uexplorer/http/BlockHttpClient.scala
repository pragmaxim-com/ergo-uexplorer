package org.ergoplatform.uexplorer.http

import io.circe.refined.*
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ApiTransaction}
import org.ergoplatform.uexplorer.{BlockId, Height, TxId}
import sttp.capabilities.zio.ZioStreams
import sttp.client3.*
import sttp.client3.circe.*
import zio.*
import scala.concurrent.duration.DurationInt
import scala.collection.immutable.ListMap

case class BlockHttpClient(metadataHttpClient: MetadataHttpClient, sttpB: SttpBackend[Task, ZioStreams]) extends Codecs {

  private val proxyUri = uri"http://proxy"

  def getBestBlockHeight: Task[Height] =
    metadataHttpClient.getMasterNodes
      .map(_.minBy(_.fullHeight).fullHeight)
      .tap(h => ZIO.log(s"Best block height is $h"))

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
      .tap(blockIds => ZIO.when(blockIds.nonEmpty)(ZIO.log(s"Retrieved ${blockIds.size} block ids from height $fromHeightIncl")))

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

  def getBlockForIdAsString(blockId: BlockId): Task[String] =
    basicRequest
      .get(proxyUri.addPath("blocks", blockId.toString))
      .response(asString)
      .responseGetRight
      .readTimeout(5.seconds)
      .send(sttpB)
      .map(_.body)

  def getBlockForId(blockId: BlockId): Task[ApiFullBlock] =
    basicRequest
      .get(proxyUri.addPath("blocks", blockId.toString))
      .response(asJson[ApiFullBlock])
      .responseGetRight
      .readTimeout(5.seconds)
      .send(sttpB)
      .map(_.body)

  def close(): Task[Unit] =
    sttpB.close()

}

object BlockHttpClient {

  def layer: ZLayer[MetadataHttpClient with SttpBackend[Task, ZioStreams], Nothing, BlockHttpClient] =
    ZLayer.fromFunction(BlockHttpClient.apply _)

}
