package org.ergoplatform.uexplorer.indexer.http

import io.circe.generic.auto._
import org.ergoplatform.uexplorer.indexer.http.MetadataHttpClient.stripUri
import org.ergoplatform.uexplorer.indexer.http.NodePool.{ConnectedPeer, PeerInfo}
import org.ergoplatform.uexplorer.indexer.{PeerAddress, ResiliencySupport, StopException}
import retry.Policy
import sttp.client3._
import sttp.client3.circe.asJson
import sttp.model.Uri
import sttp.model.Uri.{EmptyPath, QuerySegment}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Right, Success}

class MetadataHttpClient[P](val masterPeerAddress: Uri, val localNodeAddress: Uri)(implicit
  val underlyingB: SttpBackend[Future, P]
) extends ResiliencySupport {

  def bestPeerByHeight(local: Uri, remote: Uri, retryPolicy: Policy)(req: Uri => Future[PeerInfo]): Future[PeerInfo] =
    retryPolicy
      .apply { () =>
        req(local).transformWith {
          case Success(l) =>
            req(remote).map(r => List(r, l).maxBy(_.fullHeight.getOrElse(0)))
          case Failure(_) =>
            req(remote)

        }
      }(retry.Success.always, global)

  def getMasterInfo: Future[PeerInfo] =
    bestPeerByHeight(localNodeAddress, masterPeerAddress, retry.Backoff(3, 1.second)) { uri =>
      basicRequest
        .get(uri.addPath("info"))
        .response(asJson[PeerInfo])
        .responseGetRight
        .readTimeout(1.seconds)
        .send(underlyingB)
        .map(_.body)
        .transform {
          case Failure(ex) =>
            Failure(new StopException("Getting master info failed, retrying", ex))
          case Success(peerInfo) if peerInfo.fullHeight.isEmpty =>
            Failure(new StopException("Master peer full height is null", null))
          case Success(peerInfo) =>
            Success(peerInfo)
        }
    }

  def getConnectedPeers: Future[Set[ConnectedPeer]] =
    fallback(List(masterPeerAddress, localNodeAddress), retry.Backoff(3, 1.second)) { uri =>
      basicRequest
        .get(uri.addPath("peers", "connected"))
        .response(asJson[Set[ConnectedPeer]])
        .readTimeout(1.seconds)
        .send(underlyingB)
        .map(_.body)
        .map {
          case Right(connectedPeers) =>
            connectedPeers
          case Left(ex) =>
            logger.warn(s"Unable to obtain connected peers from $uri", ex)
            Set.empty[ConnectedPeer]
        }
    }

  def getPeers(masterFullHeight: Int, peerAddress: Uri): Future[Either[Uri, Uri]] =
    retry
      .Backoff(3, 1.second)
      .apply { () =>
        basicRequest
          .get(peerAddress.addPath("info"))
          .response(asJson[PeerInfo])
          .readTimeout(1.seconds)
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
