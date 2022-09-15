package org.ergoplatform.uexplorer.indexer.http

import io.circe.{Decoder, HCursor}
import org.ergoplatform.uexplorer.indexer.Utils
import sttp.model.Uri

case class PeerInfo(appVersion: String, stateType: String, fullHeight: Int, restApiUrl: Option[Uri])

object PeerInfo {

  implicit val decodeUri: Decoder[Uri] = Decoder.decodeString.emap(Uri.parse)

  implicit val decodeOpt: Decoder[Option[Uri]] =
    Decoder.decodeString.map(Uri.parse(_).map(Option(_)).getOrElse(Option.empty[Uri]))

  implicit val decoder: Decoder[PeerInfo] = (c: HCursor) => {
    for {
      stateType  <- c.downField("stateType").as[String]
      appVersion <- c.downField("appVersion").as[String]
      fullHeight <- c.downField("fullHeight").as[Option[Int]]
      restApiUri <- c.getOrElse[Option[Uri]]("restApiUrl")(Option.empty[Uri])
    } yield new PeerInfo(appVersion, stateType, fullHeight.getOrElse(0), restApiUri.map(Utils.stripUri))
  }
}

case class ConnectedPeer(restApiUrl: Option[Uri])

object ConnectedPeer {

  implicit val decodeUri: Decoder[Uri] = Decoder.decodeString.emap(Uri.parse)

  implicit val decodeOpt: Decoder[Option[Uri]] =
    Decoder.decodeString.map(Uri.parse(_).map(Option(_)).getOrElse(Option.empty[Uri]))

  implicit val decoder: Decoder[ConnectedPeer] = (c: HCursor) => {
    for {
      restApiUri <- c.getOrElse[Option[Uri]]("restApiUrl")(Option.empty[Uri])
    } yield new ConnectedPeer(restApiUri.map(Utils.stripUri))
  }

}

sealed trait Peer {
  def uri: Uri

  def weight: Int

  def fullHeight: Int
}

case class LocalNode(uri: Uri, fullHeight: Int, restApiUrl: Option[Uri]) extends Peer {
  val weight: Int = 1
}

object LocalNode {
  def fromPeerInfo(uri: Uri, pi: PeerInfo): LocalNode = LocalNode(uri, pi.fullHeight, pi.restApiUrl)
}

case class RemoteNode(uri: Uri, fullHeight: Int, restApiUrl: Option[Uri]) extends Peer {
  val weight: Int = 2
}

object RemoteNode {
  def fromPeerInfo(uri: Uri, pi: PeerInfo): RemoteNode = RemoteNode(uri, pi.fullHeight, pi.restApiUrl)
}

case class RemotePeer(uri: Uri, fullHeight: Int) extends Peer {
  val weight: Int = 3
}

object RemotePeer {
  def fromPeerInfo(uri: Uri, pi: PeerInfo): RemotePeer = RemotePeer(uri, pi.fullHeight)
}
