package org.ergoplatform.uexplorer.http

import sttp.capabilities.zio.ZioStreams
import sttp.client3.SttpBackend
import sttp.client3.httpclient.zio.HttpClientZioBackend
import sttp.client3.testing.SttpBackendStub
import zio.{Task, ZIO, ZLayer}

case class UnderlyingBackend(backend: SttpBackend[Task, ZioStreams])

object UnderlyingBackend {
  def layer: ZLayer[Any, Throwable, UnderlyingBackend] =
    HttpClientZioBackend.layer().flatMap(b => ZLayer.succeed(UnderlyingBackend(b.get)))

  def layerFor(backend: SttpBackendStub[Task, ZioStreams]): ZLayer[Any, Nothing, UnderlyingBackend] =
    ZLayer.scoped(
      ZIO.acquireRelease(ZIO.succeed(UnderlyingBackend(backend)))(b => ZIO.succeed(b.backend.close()))
    )
}
