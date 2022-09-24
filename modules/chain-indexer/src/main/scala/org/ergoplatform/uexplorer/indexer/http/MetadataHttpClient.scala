package org.ergoplatform.uexplorer.indexer.http

import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import io.circe.Decoder
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import org.ergoplatform.uexplorer.indexer.{Const, ResiliencySupport}
import retry.Policy
import sttp.client3._
import sttp.client3.circe.asJson

import scala.collection.immutable.{SortedSet, TreeSet}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

class MetadataHttpClient[P](minNodeHeight: Int = Const.MinNodeHeight)(implicit
  remoteUri: RemoteNodeUriMagnet,
  localUri: LocalNodeUriMagnet,
  system: ActorSystem[Nothing],
  underlyingB: SttpBackend[Future, P]
) extends ResiliencySupport {

  private val retryPolicy: Policy = retry.Backoff(3, 1.second)

  def getPeerInfo[T <: Peer: Decoder: UriMagnet](minHeight: Int = minNodeHeight): Future[Option[T]] =
    basicRequest
      .get(implicitly[UriMagnet[T]].uri.addPath("info"))
      .response(asJson[T])
      .responseGetRight
      .readTimeout(1.seconds)
      .send(underlyingB)
      .map(_.body)
      .transform {
        case Success(peer) if peer.fullHeight < minHeight - Const.AllowedHeightDiff || peer.stateType != "utxo" =>
          logger.debug(s"Peer has empty fullHeight or it has not utxo state : $peer")
          Success(None)
        case Success(peer) =>
          Success(Some(peer))
        case Failure(ex) =>
          logger.debug("Getting peer info failed", ex)
          Success(None)
      }

  def getAllOpenApiPeers: Future[SortedSet[Peer]] =
    getMasterNodes
      .map {
        case masterNodes if masterNodes.size > 1 =>
          val bestFullHeight = masterNodes.maxBy(_.fullHeight).fullHeight
          masterNodes.filter(_.fullHeight >= bestFullHeight - Const.AllowedHeightDiff)
        case masterNodes =>
          masterNodes
      }
      .flatMap(masterNodes =>
        getAllValidConnectedPeers(masterNodes, masterNodes.maxBy(_.fullHeight).fullHeight).map(_ ++ masterNodes)
      )

  def getMasterNodes: Future[SortedSet[Peer]] =
    retryPolicy
      .apply { () =>
        getPeerInfo[LocalNode]()
          .transformWith {
            case Success(localNode) =>
              getPeerInfo[RemoteNode]()
                .map(_.toSet[Peer] ++ localNode)
                .recover { case _ => localNode.toSet[Peer] }
            case Failure(_) | Success(None) =>
              getPeerInfo[RemoteNode]().map(_.toSet[Peer])
          }
          .map(_.to[TreeSet])
      }(retry.Success.apply(_.nonEmpty), global)

  def getConnectedPeers(masterPeer: Peer): Future[Set[ConnectedPeer]] =
    basicRequest
      .get(masterPeer.uri.addPath("peers", "connected"))
      .response(asJson[Set[ConnectedPeer]])
      .responseGetRight
      .readTimeout(1.seconds)
      .send(underlyingB)
      .map(_.body)

  def getAllValidConnectedPeers(masterPeers: SortedSet[Peer], bestFullHeight: Int): Future[SortedSet[Peer]] =
    Source(masterPeers)
      .mapAsync(2)(getConnectedPeers)
      .mapConcat(identity)
      .collect { case p if p.restApiUrl.isDefined => RemotePeerUriMagnet(p.restApiUrl.get) }
      .mapAsync(1) { implicit m: RemotePeerUriMagnet => getPeerInfo[RemotePeer](bestFullHeight) }
      .mapConcat(_.toList)
      .runWith(Sink.collection[Peer, TreeSet[Peer]])

  def close(): Future[Unit] =
    underlyingB.close()
}

object MetadataHttpClient {

  def apply[P](
    conf: ChainIndexerConf
  )(implicit underlyingB: SttpBackend[Future, P], system: ActorSystem[Nothing]): MetadataHttpClient[P] = {
    implicit val localNodeUriMagnet: LocalNodeUriMagnet   = conf.localUriMagnet
    implicit val remoteNodeUriMagnet: RemoteNodeUriMagnet = conf.remoteUriMagnet
    new MetadataHttpClient[P]()
  }
}
