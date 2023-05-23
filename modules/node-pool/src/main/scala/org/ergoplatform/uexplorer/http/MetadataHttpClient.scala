package org.ergoplatform.uexplorer.http

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import io.circe.Decoder
import org.ergoplatform.uexplorer.Const
import retry.Policy
import sttp.client3.*
import sttp.client3.circe.asJson

import scala.collection.immutable.{SortedSet, TreeSet}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}
import org.ergoplatform.uexplorer.ResiliencySupport
import org.ergoplatform.uexplorer.Const.EpochLength

class MetadataHttpClient[P](minNodeHeight: Int = EpochLength * 800)(implicit
  remoteUri: RemoteNodeUriMagnet,
  localUri: LocalNodeUriMagnet,
  system: ActorSystem[Nothing],
  underlyingB: SttpBackend[Future, P]
) extends ResiliencySupport {

  private val allowedHeightDiff = 10

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
        case Success(peer) if peer.fullHeight < minHeight - allowedHeightDiff || peer.stateType != "utxo" =>
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
          masterNodes.filter(_.fullHeight >= bestFullHeight - allowedHeightDiff)
        case masterNodes =>
          masterNodes
      }
      .flatMap {
        case masterNodes if masterNodes.nonEmpty =>
          getAllValidConnectedPeers(masterNodes, masterNodes.maxBy(_.fullHeight).fullHeight).map(_ ++ masterNodes)
        case _ =>
          Future.successful(TreeSet.empty[Peer])
      }

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
          .map(_.to(TreeSet.evidenceIterableFactory[Peer]))
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

  def close(): Future[Unit] = {
    logger.info(s"Stopping Metadata http client")
    underlyingB.close()
  }
}

object MetadataHttpClient {

  def apply[P](
    implicit localNodeUriMagnet: LocalNodeUriMagnet, remoteNodeUriMagnet: RemoteNodeUriMagnet, underlyingB: SttpBackend[Future, P], system: ActorSystem[Nothing]
  ): MetadataHttpClient[P] = {
    val metadataClient = new MetadataHttpClient[P]()
    CoordinatedShutdown(system).addTask(
      CoordinatedShutdown.PhaseServiceStop,
      "stop-metadata-http-client"
    ) { () =>
      metadataClient.close().map(_ => Done)
    }
    metadataClient
  }
}