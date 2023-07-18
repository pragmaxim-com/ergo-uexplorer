package org.ergoplatform.uexplorer.http

import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder
import org.ergoplatform.uexplorer.Const.EpochLength
import org.ergoplatform.uexplorer.Const
import sttp.client3.*
import sttp.client3.circe.asJson
import zio.stream.ZStream
import zio.*
import nl.vroste.rezilience.*

import scala.concurrent.duration
import scala.collection.immutable.{SortedSet, TreeSet}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}
import nl.vroste.rezilience.Retry.Schedules
import sttp.capabilities.zio.ZioStreams
import sttp.client3.httpclient.zio.{HttpClientZioBackend, SttpClient}

import scala.concurrent.duration.FiniteDuration
import sttp.model.Uri

case class MetadataHttpClient(underlying: UnderlyingBackend, conf: NodePoolConf) extends LazyLogging {

  private val allowedHeightDiff = 20

  implicit private val remoteUriMagnet: RemoteNodeUriMagnet = conf.remoteUriMagnet
  implicit private val localUriMagnet: LocalNodeUriMagnet   = conf.localUriMagnet

  private def getPeerInfo[T <: Peer: Decoder: UriMagnet](minHeight: Option[Int] = None): Task[Option[T]] =
    basicRequest
      .get(implicitly[UriMagnet[T]].uri.addPath("info"))
      .response(asJson[T])
      .responseGetRight
      .readTimeout(FiniteDuration(1, duration.SECONDS))
      .send(underlying.backend)
      .map(_.body)
      .fold(
        _ => Option.empty[T],
        peer =>
          if (peer.fullHeight < minHeight.getOrElse(0) || peer.stateType != "utxo") {
            logger.warn(s"Peer has empty fullHeight or it has not utxo state : $peer")
            None
          } else {
            logger.info(s"Found valid peer with height ${peer.fullHeight}")
            Some(peer)
          }
      )

  def getLocalNodeInfo: Task[Option[LocalNode]]   = getPeerInfo[LocalNode]()
  def getRemoteNodeInfo: Task[Option[RemoteNode]] = getPeerInfo[RemoteNode]()

  def getAllOpenApiPeers: Task[SortedSet[Peer]] =
    getMasterNodes
      .map {
        case masterNodes if masterNodes.size > 1 =>
          val bestFullHeight = masterNodes.maxBy(_.fullHeight).fullHeight
          masterNodes.filter(_.fullHeight >= bestFullHeight - allowedHeightDiff)
        case masterNodes =>
          masterNodes
      }
      .flatMap {
        case masterNodes if masterNodes.nonEmpty =>
          getAllValidConnectedPeers(masterNodes, masterNodes.maxBy(_.fullHeight).fullHeight)
            .map(_ ++ masterNodes)
        case _ =>
          ZIO.succeed(TreeSet.empty[Peer])
      }

  def getMasterNodes: Task[TreeSet[Peer]] =
    getPeerInfo[LocalNode](None)
      .flatMap {
        _.fold(getPeerInfo[RemoteNode](None).map(_.toSet[Peer])) { localNode =>
          getPeerInfo[RemoteNode](None)
            .fold(
              _ => TreeSet[Peer](localNode),
              _.toSet[Peer] + localNode
            )
        }
      }
      .map(_.to(TreeSet.evidenceIterableFactory[Peer]))
      .filterOrFail(_.exists(_.fullHeight > 1))(new IllegalStateException("There must be at least one live master node"))
      .logError("Unable to get any master node, retrying...")
      .retry(Schedule.exponential(1.seconds, 2.0).upTo(1.minute))

  def getConnectedPeers(masterPeer: Peer): Task[Set[ConnectedPeer]] =
    basicRequest
      .get(masterPeer.uri.addPath("peers", "connected"))
      .response(asJson[Set[ConnectedPeer]])
      .responseGetRight
      .readTimeout(FiniteDuration(1, duration.SECONDS))
      .send(underlying.backend)
      .map(_.body)

  private def getAllValidConnectedPeers(masterPeers: SortedSet[Peer], bestFullHeight: Int): Task[SortedSet[Peer]] =
    ZStream
      .fromIterable(masterPeers)
      .mapZIOPar(2)(getConnectedPeers)
      .mapConcat(identity)
      .collect { case p if p.restApiUrl.isDefined => RemotePeerUriMagnet(p.restApiUrl.get) }
      .mapZIOPar(1) { implicit m: RemotePeerUriMagnet => getPeerInfo[RemotePeer](Some(bestFullHeight)) }
      .mapConcat(_.toList)
      .runFold(TreeSet.empty[Peer]) { case (acc, p) => acc + p }

}

object MetadataHttpClient {

  def layer: ZLayer[UnderlyingBackend with NodePoolConf, Nothing, MetadataHttpClient] =
    ZLayer.fromFunction(MetadataHttpClient.apply _)

}
