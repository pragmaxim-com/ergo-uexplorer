package org.ergoplatform.uexplorer.indexer.http

import com.typesafe.scalalogging.LazyLogging
import io.circe.generic.auto._
import NodePool.{ConnectedPeer, PeerInfo}
import org.ergoplatform.uexplorer.indexer.http.MetadataHttpClient.stripUri
import org.ergoplatform.uexplorer.indexer.{PeerAddress, Resiliency}
import sttp.client3._
import sttp.client3.circe.asJson
import sttp.model.Uri
import sttp.model.Uri.{EmptyPath, QuerySegment}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Right, Success}

class MetadataHttpClient[P](val masterPeerAddr: Uri)(implicit val underlyingB: SttpBackend[Future, P]) extends LazyLogging {

  def getMasterInfo: Future[PeerInfo] =
    retry
      .Backoff(3, 1.second)
      .apply { () =>
        basicRequest
          .get(masterPeerAddr.addPath("info"))
          .response(asJson[PeerInfo])
          .responseGetRight
          .readTimeout(1.seconds)
          .send(underlyingB)
          .map(_.body)
          .transform {
            case Failure(ex) =>
              Failure(new Resiliency.StopException("Getting master info failed, retrying", ex))
            case Success(peerInfo) if peerInfo.fullHeight.isEmpty =>
              Failure(new Resiliency.StopException("Master peer full height is null", null))
            case Success(peerInfo) =>
              Success(peerInfo)
          }
      }(retry.Success.always, global)

  def getConnectedPeers: Future[Set[ConnectedPeer]] =
    retry
      .Backoff(3, 1.second)
      .apply { () =>
        basicRequest
          .get(masterPeerAddr.addPath("peers", "connected"))
          .response(asJson[Set[ConnectedPeer]])
          .readTimeout(2.seconds)
          .send(underlyingB)
          .map(_.body)
          .map {
            case Right(connectedPeers) =>
              connectedPeers
            case Left(ex) =>
              logger.warn(s"Unable to obtain connected peers from $masterPeerAddr", ex)
              Set.empty[ConnectedPeer]
          }
      }(retry.Success.always, global)

  def getPeers(masterFullHeight: Int, peerAddress: Uri): Future[Either[Uri, Uri]] =
    retry
      .Backoff(3, 1.second)
      .apply { () =>
        basicRequest
          .get(peerAddress.addPath("info"))
          .response(asJson[PeerInfo])
          .readTimeout(5.seconds)
          .send(underlyingB)
          .map(_.body)
          .transform {
            case Success(Right(PeerInfo(_, "utxo", Some(fullHeight), Some(peerNodeAddr))))
                if fullHeight >= masterFullHeight =>
              Success(Right(stripUri(Uri.parse(peerNodeAddr).right.get))) // TODO we expect Node to share only valid URIs
            case Success(_) | Failure(_) =>
              Success(Left(peerAddress))
          }
      }(retry.Success.always, global)

  def getValidAndInvalidPeers(masterPeerInfo: PeerInfo): Future[(Set[Uri], Set[Uri])] =
    getConnectedPeers
      .map { peers =>
        masterPeerInfo.restApiUrl.toSet.flatMap { addr: PeerAddress => Uri.parse(addr).toOption.map(stripUri) } ++
        peers.collect { case ConnectedPeer(_, Some(restApiUrl)) => Uri.parse(restApiUrl).toOption }.flatten[Uri]
      }
      .flatMap { openApiPeers =>
        Future
          .sequence(openApiPeers.map(pa => getPeers(masterPeerInfo.fullHeight.get, pa)))
          .map { responses =>
            responses
              .partition(_.isRight) match {
              case (rights, lefts) =>
                rights.map(_.right.get) -> lefts.map(_.left.get)
            }
          }
      }

}

object MetadataHttpClient {

  def stripUri(uri: Uri): Uri =
    uri.copy(
      pathSegments    = EmptyPath,
      querySegments   = List.empty[QuerySegment],
      fragmentSegment = Option.empty
    )

  def copyUri(origUri: Uri, newUri: Uri): Uri =
    newUri.copy(
      pathSegments    = origUri.pathSegments,
      querySegments   = origUri.querySegments,
      fragmentSegment = origUri.fragmentSegment
    )

}
