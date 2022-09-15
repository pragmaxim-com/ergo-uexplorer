package org.ergoplatform.uexplorer.indexer.http

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import org.ergoplatform.uexplorer.indexer.{Const, ResiliencySupport}
import retry.Policy
import sttp.client3._
import sttp.client3.circe.asJson
import sttp.model.Uri

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

class MetadataHttpClient[P](val masterPeerAddress: Uri, val localNodeAddress: Uri)(implicit
  val underlyingB: SttpBackend[Future, P],
  mat: Materializer
) extends ResiliencySupport {

  private val retryPolicy: Policy = retry.Backoff(3, 1.second)

  def getBestBlockHeight: Future[Int] = getMasterNodes.map(_.maxBy(_.fullHeight).fullHeight)

  def getPeerInfo[T](uri: Uri)(constructor: (Uri, PeerInfo) => T): Future[Option[T]] =
    basicRequest
      .get(uri.addPath("info"))
      .response(asJson[PeerInfo])
      .responseGetRight
      .readTimeout(1.seconds)
      .send(underlyingB)
      .map(_.body)
      .transform {
        case Success(peerInfo) if peerInfo.fullHeight < Const.MinNodeHeight || peerInfo.stateType != "utxo" =>
          logger.warn(s"Peer has empty fullHeight or it has not utxo state : $peerInfo")
          Success(None)
        case Success(peerInfo) =>
          Success(Some(constructor(uri, peerInfo)))
        case Failure(ex) =>
          logger.warn("Getting peer info failed, retrying", ex)
          Success(None)
      }

  def getAllOpenApiPeers: Future[Set[Peer]] =
    getMasterNodes
      .map {
        case masterNodes if masterNodes.size > 1 =>
          val bestFullHeight = masterNodes.maxBy(_.fullHeight).fullHeight
          masterNodes.filter(_.fullHeight >= bestFullHeight - Const.AllowedHeightDiff)
        case masterNodes =>
          masterNodes
      }
      .flatMap(masterNodes =>
        getConnectedPeers(masterNodes, masterNodes.maxBy(_.fullHeight).fullHeight).map(_ ++ masterNodes)
      )

  def getMasterNodes: Future[Set[Peer]] =
    retryPolicy
      .apply { () =>
        getPeerInfo(localNodeAddress)(LocalNode.fromPeerInfo)
          .transformWith {
            case Success(localNode) =>
              getPeerInfo(masterPeerAddress)(RemoteNode.fromPeerInfo)
                .map(_.toSet[Peer] ++ localNode)
                .recover { case _ => localNode.toSet[Peer] }
            case Failure(_) | Success(None) =>
              getPeerInfo(masterPeerAddress)(RemoteNode.fromPeerInfo).map(_.toSet[Peer])
          }
      }(retry.Success.apply(_.nonEmpty), global)

  def getConnectedPeers(masterPeers: Set[Peer], bestFullHeight: Int): Future[Set[Peer]] =
    Source(masterPeers)
      .mapAsync(2) { peer =>
        basicRequest
          .get(peer.uri.addPath("peers", "connected"))
          .response(asJson[Set[ConnectedPeer]])
          .responseGetRight
          .readTimeout(1.seconds)
          .send(underlyingB)
          .map(_.body)
      }
      .mapConcat(identity)
      .collect { case p if p.restApiUrl.isDefined => p.restApiUrl.get }
      .mapAsync(1)(getPeerInfo(_)(RemotePeer.fromPeerInfo))
      .mapConcat {
        case Some(remotePeer) if remotePeer.fullHeight >= bestFullHeight - 5 =>
          List(remotePeer)
        case _ =>
          List.empty
      }
      .runWith(Sink.collection[Peer, Set[Peer]])

  def close(): Future[Unit] =
    underlyingB.close()
}
