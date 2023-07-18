package org.ergoplatform.uexplorer.http

import org.ergoplatform.uexplorer.config.ExplorerConfig
import org.ergoplatform.uexplorer.http.{Rest, TestSupport}
import sttp.client3.testing.SttpBackendStub
import sttp.client3.*
import sttp.model.StatusCode
import org.ergoplatform.uexplorer.http.LocalNodeUriMagnet
import org.ergoplatform.uexplorer.http.RemoteNodeUriMagnet
import org.ergoplatform.uexplorer.http.MetadataHttpClient
import org.ergoplatform.uexplorer.http.RemoteNode
import org.ergoplatform.uexplorer.http.RemotePeer
import org.ergoplatform.uexplorer.http.LocalNode
import org.ergoplatform.uexplorer.http.RemotePeerUriMagnet
import org.ergoplatform.uexplorer.http.ConnectedPeer
import sttp.capabilities.zio.ZioStreams
import zio.*
import zio.test.*
import zio.test.Assertion.*
import sttp.client3.httpclient.zio.HttpClientZioBackend
import zio.config.typesafe.TypesafeConfigProvider
import zio.test.ZIOSpecDefault

object MetaHttpClientSpec extends ZIOSpecDefault with TestSupport {

  private val appVersion = "4.0.42"
  private val stateType  = "utxo"

  def stubLayers(
    fn: SttpBackendStub[Task, ZioStreams] => SttpBackendStub[Task, ZioStreams]
  ): ZLayer[Any, Config.Error, MetadataHttpClient] =
    ZLayer.scoped(
      ZIO.acquireRelease(ZIO.succeed(UnderlyingBackend(fn(HttpClientZioBackend.stub))))(b => ZIO.succeed(b.backend.close()))
    ) ++ NodePoolConf.layer >>> MetadataHttpClient.layer

  def spec: Spec[Any, Throwable] =
    suite("meta")(
      test(Rest.info.sync) {
        ZIO
          .serviceWithZIO[MetadataHttpClient] { client =>
            for {
              conf           <- NodePoolConf.configIO
              f1             <- client.getMasterNodes.map(_.toList).fork
              f2             <- client.getAllOpenApiPeers.map(_.toList).fork
              _              <- TestClock.adjust(2.minute)
              localsAndPeers <- f1.zip(f2).join
              localNode      <- client.getLocalNodeInfo
              remoteNode     <- client.getRemoteNodeInfo
            } yield assertTrue(
              localNode == Option(LocalNode(conf.nodeAddressToInitFrom, appVersion, stateType, 4150)),
              remoteNode == Option(RemoteNode(conf.peerAddressToPollFrom, appVersion, stateType, 4150)),
              localsAndPeers._1.map(_.uri) == List(conf.nodeAddressToInitFrom, conf.peerAddressToPollFrom),
              localsAndPeers._2.map(_.uri) == List(conf.nodeAddressToInitFrom, conf.peerAddressToPollFrom, uri"http://peer")
            )
          }
          .provide(
            stubLayers(
              _.whenRequestMatches(_.uri.path.startsWith(List("peers", "connected")))
                .thenRespond(Response.ok[String](getConnectedPeers))
                .whenAnyRequest
                .thenRespondCyclicResponses(Response.ok[String](getPeerInfo(Rest.info.sync)))
            )
          )
      },
      test(Rest.info.woRestApiAndWoFullHeight) {
        ZIO
          .serviceWithZIO[MetadataHttpClient] { client =>
            for {
              f1 <- client.getMasterNodes.map(_.toList).fork
              _  <- TestClock.adjust(2.minute)
              _  <- f1.join
            } yield TestResult.any() // @@ TestAspect.failing
          }
          .provide(
            stubLayers(
              _.whenAnyRequest
                .thenRespondCyclicResponses(Response.ok[String](getPeerInfo(Rest.info.woRestApiAndWoFullHeight)))
            )
          )
      } @@ TestAspect.failing,
      test("client should return only valid connected peers") {
        ZIO
          .serviceWithZIO[MetadataHttpClient] { client =>
            for {
              conf <- NodePoolConf.configIO
              f1   <- client.getConnectedPeers(LocalNode(conf.nodeAddressToInitFrom, appVersion, stateType, 839249)).fork
              _    <- TestClock.adjust(2.minute)
              connectedPeers <- f1.join
            } yield assertTrue(
              connectedPeers.collect { case ConnectedPeer(Some(restApiUrl)) => restApiUrl } == Set(uri"http://peer")
            )
          }
          .provide(
            stubLayers(
              _.whenRequestMatches(_.uri.path.startsWith(List("peers", "connected")))
                .thenRespond(Response.ok[String](getConnectedPeers))
            )
          )
      },
      test("client should handle peerInfo error by returning None") {
        ZIO
          .serviceWithZIO[MetadataHttpClient] { client =>
            for {
              r               <- client.getLocalNodeInfo.fork
              _               <- TestClock.adjust(2.minute)
              localNodeOption <- r.join
            } yield assertTrue(localNodeOption.isEmpty)
          }
          .provide(
            stubLayers(
              _.whenAnyRequest
                .thenRespondCyclicResponses(
                  Response("error", StatusCode.InternalServerError, "Something went wrong")
                )
            )
          )
      }
    )

}
