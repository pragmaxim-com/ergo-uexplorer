package org.ergoplatform.uexplorer.http

import org.ergoplatform.uexplorer.config.ExplorerConfig
import zio.{Runtime, *}
import zio.test.*
import zio.test.Assertion.*
import sttp.capabilities
import sttp.client3.*
import sttp.client3.testing.SttpBackendStub
import sttp.model.StatusCode

import scala.collection.immutable.TreeSet
import scala.concurrent.Future
import scala.util.Success
import org.ergoplatform.uexplorer.http.LocalNode
import org.ergoplatform.uexplorer.http.RemoteNode
import org.ergoplatform.uexplorer.http.RemotePeer
import org.ergoplatform.uexplorer.http.Peer
import org.ergoplatform.uexplorer.http.SttpNodePoolBackend
import org.ergoplatform.uexplorer.http.NodePool
import org.ergoplatform.uexplorer.http.NodePoolState
import sttp.capabilities.zio.ZioStreams
import sttp.client3.httpclient.zio.HttpClientZioBackend
import zio.config.*
import zio.*
import zio.config.typesafe.TypesafeConfigProvider

object SttpNodePoolBackendSpec extends ZIOSpecDefault with TestSupport {

  private val appVersion = "4.0.42"
  private val stateType  = "utxo"

  private val localNode  = LocalNode(uri"http://localNode", appVersion, stateType, 0)
  private val remoteNode = RemoteNode(uri"http://remoteNode", appVersion, stateType, 0)
  private val remotePeer = RemotePeer(uri"http://remotePeer", appVersion, stateType, 0)

  private val proxyUri = uri"http://proxy"

  def stubLayers(
    fn: SttpBackendStub[Task, ZioStreams] => SttpBackendStub[Task, ZioStreams]
  ): ZLayer[Any, Config.Error, NodePool with SttpNodePoolBackend] =
    ZLayer.scoped(
      ZIO.acquireRelease(ZIO.succeed(UnderlyingBackend(fn(HttpClientZioBackend.stub))))(b => ZIO.succeed(b.backend.close()))
    ) >+> NodePoolConf.layer >+> MetadataHttpClient.layer >+> NodePool.layer >+> SttpNodePoolBackend.layer

  def spec =
    suite("nodepool-backend")(
      test("node pool backend should try all peers in given order until one succeeds") {
        implicit val testingBackend: SttpBackendStub[Task, ZioStreams] = HttpClientZioBackend.stub.whenAnyRequest
          .thenRespondCyclicResponses(
            Response("error", StatusCode.InternalServerError, "Local node failed"),
            Response("error", StatusCode.InternalServerError, "Remote node not available"),
            Response.ok[String]("Remote peer available")
          )

        def proxyRequest(p: Peer) =
          basicRequest.get(p.uri).responseGetRight.send(testingBackend).map(_.body)

        SttpNodePoolBackend
          .fallbackQuery[String](List(localNode, remoteNode, remotePeer), TreeSet.empty)(proxyRequest)
          .map { case (invalidPeers, response) =>
            assertTrue(response == Success("Remote peer available"), invalidPeers == TreeSet(localNode, remoteNode))
          }
      },
      test("node pool backend should swap uri in request") {
        assertTrue(SttpNodePoolBackend.swapUri(basicRequest.get(proxyUri), localNode.uri) == basicRequest.get(localNode.uri))
      },
      test("node pool backend should get peers from node pool to proxy request to and invalidate failing peers") {
        (for {
          nodePool <- ZIO.service[NodePool]
          x        <- nodePool.updateOpenApiPeers(TreeSet(localNode, remoteNode, remotePeer))
          r <- ZIO
                 .serviceWithZIO[SttpNodePoolBackend](_.send(basicRequest.get(proxyUri).responseGetRight))
                 .map(_.body)
                 .flatMap { resp =>
                   nodePool.getAvailablePeers.map { availablePeers =>
                     assertTrue(resp == "Remote peer available", availablePeers.toList == List(remoteNode, remotePeer))
                   }
                 }
        } yield r)
          .provide(
            stubLayers(
              _.whenRequestMatches(_.uri == localNode.uri)
                .thenRespond(Response("error", StatusCode.InternalServerError, "Local node failed"))
                .whenRequestMatches(_.uri == remoteNode.uri)
                .thenRespond("Remote peer available")
                .whenRequestMatches(_.uri == proxyUri)
                .thenRespondServerError()
            )
          )
      }
    )
}
