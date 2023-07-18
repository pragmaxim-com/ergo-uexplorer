package org.ergoplatform.uexplorer.http

import org.ergoplatform.uexplorer.config.ExplorerConfig
import sttp.model.Uri
import zio.*
import zio.config.magnolia._
import zio.config.typesafe._

case class NodePoolConf(
  nodeAddressToInitFrom: Uri,
  peerAddressToPollFrom: Uri
) {
  def localUriMagnet: LocalNodeUriMagnet   = LocalNodeUriMagnet(nodeAddressToInitFrom)
  def remoteUriMagnet: RemoteNodeUriMagnet = RemoteNodeUriMagnet(peerAddressToPollFrom)
}

object NodePoolConf {
  implicit val uriConfig: DeriveConfig[Uri] =
    DeriveConfig[String].mapOrFail(uri => Uri.parse(uri).left.map(msg => zio.Config.Error.Unsupported(message = msg)))

  val config: zio.Config[NodePoolConf] =
    deriveConfig[NodePoolConf].nested("nodePool")

  def configIO: IO[zio.Config.Error, NodePoolConf] = ExplorerConfig().load[NodePoolConf](config)

  def layer: ZLayer[Any, zio.Config.Error, NodePoolConf] = ZLayer.fromZIO(configIO)

}
