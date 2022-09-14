package org.ergoplatform.uexplorer.indexer.http

import io.circe.generic.auto._
import org.ergoplatform.uexplorer.indexer.http.NodePool._
import org.ergoplatform.uexplorer.indexer.{Const, ResiliencySupport, StopException, Utils}
import retry.Policy
import sttp.client3._
import sttp.client3.circe.asJson
import sttp.model.Uri

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

class MetadataHttpClient[P](val masterPeerAddress: Uri, val localNodeAddress: Uri)(implicit
  val underlyingB: SttpBackend[Future, P]
) extends ResiliencySupport {

  def getBestBlockHeight: Future[Int] = getOpenApiPeers.map(_.maxBy(_.fullHeight).fullHeight)

  def getPeerInfo(uri: Uri): Future[PeerInfo] =
    basicRequest
      .get(uri.addPath("info"))
      .response(asJson[PeerInfo])
      .responseGetRight
      .readTimeout(1.seconds)
      .send(underlyingB)
      .map(_.body)

  def getOpenApiPeers: Future[Set[Peer]] =
    filterMasterPeers(localNodeAddress, masterPeerAddress, retry.Backoff(3, 1.second)) { uri =>
      getPeerInfo(uri)
        .transform {
          case Failure(ex) =>
            Failure(new StopException("Getting master info failed, retrying", ex))
          case Success(peerInfo) if peerInfo.fullHeight.isEmpty =>
            Failure(new StopException("Master peer full height is null", null))
          case Success(peerInfo) =>
            Success(peerInfo)
        }
    }

  def filterMasterPeers(local: Uri, remote: Uri, retryPolicy: Policy)(req: Uri => Future[PeerInfo]): Future[Set[Peer]] =
    retryPolicy
      .apply { () =>
        req(local)
          .map(pi => LocalNode(local, pi.getFullHeight, pi.getRestApiUri))
          .transformWith {
            case Success(localNode) =>
              req(remote)
                .map(pi => RemoteNode(remote, pi.getFullHeight, pi.getRestApiUri))
                .map { remoteNode =>
                  val bothNodes      = Set[Peer](localNode, remoteNode)
                  val bestFullHeight = bothNodes.maxBy(_.fullHeight).fullHeight
                  bothNodes.filter(_.fullHeight >= bestFullHeight - Const.AllowedHeightDiff)
                }
                .recover { case _ => Set[Peer](localNode).filter(_.fullHeight > Const.MinNodeHeight) }
            case Failure(_) =>
              req(remote).flatMap {
                case pi if pi.getFullHeight > Const.MinNodeHeight =>
                  val masterPeers = Set[Peer](
                    RemoteNode(remote, pi.getFullHeight, pi.getRestApiUri)
                  )
                  getConnectedPeers(masterPeers, masterPeers.maxBy(_.fullHeight).fullHeight).map(masterPeers ++ _)
                case _ =>
                  Future.successful(Set.empty[Peer])
              }
          }
      }(retry.Success.always, global)

  def getRemotePeer(masterFullHeight: Int)(peerAddress: Uri): Future[Option[RemotePeer]] =
    retry
      .Backoff(3, 1.second)
      .apply { () =>
        getPeerInfo(peerAddress)
          .transform {
            case Success(PeerInfo(_, "utxo", Some(fullHeight), Some(peerNodeAddr))) if fullHeight >= masterFullHeight - 5 =>
              // TODO we expect Node to share only valid URIs
              Success(Uri.parse(peerNodeAddr).right.toOption.map(Utils.stripUri).map(RemotePeer(_, fullHeight)))
            case Failure(_) =>
              Success(None)
          }
      }(retry.Success.always, global)

  def getConnectedPeers(masterPeers: Set[Peer], bestFullHeight: Int): Future[Set[RemotePeer]] =
    Future
      .sequence(
        masterPeers.map { peer =>
          basicRequest
            .get(peer.uri.addPath("peers", "connected"))
            .response(asJson[Set[ConnectedPeer]])
            .responseGetRight
            .readTimeout(1.seconds)
            .send(underlyingB)
            .map(_.body)
            .transformWith {
              case Success(connectedPeers) =>
                Future
                  .sequence(connectedPeers.flatMap(_.getRestApiUri).map(getRemotePeer(bestFullHeight)))
                  .map(_.flatten)
              case Failure(ex) =>
                logger.warn(s"Unable to obtain connected peers from ${peer.uri}", ex)
                Future.successful(Set.empty[RemotePeer])
            }
        }
      )
      .map(_.flatten)

  def close(): Future[Unit] =
    underlyingB.close()
}
