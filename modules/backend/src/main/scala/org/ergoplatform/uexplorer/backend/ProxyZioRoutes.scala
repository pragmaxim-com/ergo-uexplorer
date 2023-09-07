package org.ergoplatform.uexplorer.backend

import org.ergoplatform.uexplorer.http.NodePool
import zio.*
import zio.http.*

object ProxyZioRoutes extends ZioRoutes with Codecs:

  def apply(): Http[Client with NodePool, Throwable, Request, Response] =
    Http.collectZIO[Request] { case req =>
      for
        bestPeerOpt <- ZIO.serviceWithZIO[NodePool](_.getBestPeer)
        bestPeer    <- bestPeerOpt.fold(ZIO.fail(new IllegalStateException(s"Peers unavailable in node pool")))(ZIO.succeed)
        response    <- Client.request(req.copy(url = URL.fromURI(bestPeer.uri.toJavaUri).map(_.withPath(req.path)).get))
      yield response
    }
