package org.ergoplatform.uexplorer.http

import io.circe.{Decoder, HCursor}
import org.ergoplatform.uexplorer.{Height, Utils}
import sttp.model.Uri

import java.net.{URI, URL}
import scala.util.Try

case class ConnectedPeer(restApiUrl: Option[Uri])

object ConnectedPeer {

  private def hasFragments(uri: URI) =
    Option(uri.getQuery).exists(_.nonEmpty) ||
    Option(uri.getPath).exists(_.nonEmpty) ||
    Option(uri.getFragment).exists(_.nonEmpty)

  private def parseUri(urlStr: String): Try[Uri] =
    Try(new URL(urlStr).toURI)
      .filter(uri => !hasFragments(uri))
      .map(uri => Uri.unsafeParse(uri.toString))

  implicit val decodeOpt: Decoder[Option[Uri]] =
    Decoder.decodeString.emap { uriStr =>
      Right(
        parseUri(uriStr)
          .fold[Option[Uri]](
            _ => Option.empty,
            uri => Option(Utils.stripUri(uri))
          )
      )
    }

  implicit val decoder: Decoder[ConnectedPeer] = (c: HCursor) => {
    for {
      restApiUri <- c.getOrElse[Option[Uri]]("restApiUrl")(Option.empty[Uri])
    } yield new ConnectedPeer(restApiUri.map(Utils.stripUri))
  }
}

sealed trait Peer {
  def uri: Uri
  def weight: Int
  def stateType: Peer.StateType
  def appVersion: Peer.AppVersion
  def fullHeight: Int
}

object Peer {
  type StateType  = String
  type AppVersion = String

  implicit def ascWeightOrdering[P <: Peer]: Ordering[P] =
    Ordering.by[P, Int](_.weight)

  def baseDecoder: Decoder[(StateType, AppVersion, Height)] = (c: HCursor) => {
    for {
      stateType  <- c.downField("stateType").as[StateType]
      appVersion <- c.downField("appVersion").as[AppVersion]
      fullHeight <- c.downField("fullHeight").as[Option[Height]]
    } yield (appVersion, stateType, fullHeight.getOrElse(0))
  }

  implicit def remoteNodeDecoder(implicit m: UriMagnet[RemoteNode]): Decoder[RemoteNode] = (c: HCursor) =>
    baseDecoder(c).map { case (stateType, appVersion, fullHeight) =>
      RemoteNode(m.uri, stateType, appVersion, fullHeight)
    }

  implicit def remotePeerDecoder(implicit m: UriMagnet[RemotePeer]): Decoder[RemotePeer] = (c: HCursor) =>
    baseDecoder(c).map { case (stateType, appVersion, fullHeight) =>
      RemotePeer(m.uri, stateType, appVersion, fullHeight)
    }

  implicit def localNodeDecoder(implicit m: UriMagnet[LocalNode]): Decoder[LocalNode] = (c: HCursor) =>
    baseDecoder(c).map { case (stateType, appVersion, fullHeight) =>
      LocalNode(m.uri, stateType, appVersion, fullHeight)
    }

}

sealed trait UriMagnet[T] {
  def uri: Uri
}

case class LocalNodeUriMagnet(uri: Uri) extends UriMagnet[LocalNode]
case class RemoteNodeUriMagnet(uri: Uri) extends UriMagnet[RemoteNode]
case class RemotePeerUriMagnet(uri: Uri) extends UriMagnet[RemotePeer]

case class LocalNode(uri: Uri, appVersion: String, stateType: String, fullHeight: Int) extends Peer {
  val weight: Int = 1
}

case class RemoteNode(uri: Uri, appVersion: String, stateType: String, fullHeight: Int) extends Peer {
  val weight: Int = 2
}

case class RemotePeer(uri: Uri, appVersion: String, stateType: String, fullHeight: Int) extends Peer {
  val weight: Int = 3
}
