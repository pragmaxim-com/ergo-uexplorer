package org.ergoplatform.uexplorer.indexer.http

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import sttp.client3.testing.SttpBackendStub
import sttp.client3._
import sttp.model.StatusCode

import scala.concurrent.Future
import scala.io.Source

class MetaHttpClientSpec extends AsyncFreeSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  implicit private val sys: ActorSystem[_] = ActorTestKit().internalSystem

  implicit private val localNodeUriMagnet: LocalNodeUriMagnet   = LocalNodeUriMagnet(uri"http://local")
  implicit private val remoteNodeUriMagnet: RemoteNodeUriMagnet = RemoteNodeUriMagnet(uri"http://remote")
  implicit private val remotePeerUriMagnet: RemotePeerUriMagnet = RemotePeerUriMagnet(uri"http://peer")

  private val appVersion = "4.0.42"
  private val stateType  = "utxo"

  private val withRestApiAndFullHeight = "with-rest-api-and-full-height.json"
  private val woRestApiAndFullHeight   = "wo-rest-api-and-full-height.json"
  private val woRestApiAndWoFullHeight = "wo-rest-api-and-wo-full-height.json"

  override def afterAll(): Unit = {
    super.afterAll()
    sys.terminate()
  }

  private def getPeerInfo(fileName: String): String =
    Source
      .fromInputStream(Thread.currentThread().getContextClassLoader.getResourceAsStream(s"info/$fileName"))
      .mkString

  private def getConnectedPeers: String =
    Source
      .fromInputStream(Thread.currentThread().getContextClassLoader.getResourceAsStream("peers/connected.json"))
      .mkString

  withRestApiAndFullHeight in {
    implicit val testingBackend: SttpBackendStub[Future, _] = SttpBackendStub.asynchronousFuture.whenAnyRequest
      .thenRespondCyclicResponses(
        Response.ok[String](getPeerInfo(withRestApiAndFullHeight)),
        Response.ok[String](getPeerInfo(withRestApiAndFullHeight)),
        Response.ok[String](getPeerInfo(withRestApiAndFullHeight))
      )
    val client = new MetadataHttpClient()
    client.getPeerInfo[LocalNode].map { node =>
      node shouldBe Option(LocalNode(localNodeUriMagnet.uri, appVersion, stateType, 839249))
    }
    client.getPeerInfo[RemoteNode].map { node =>
      node shouldBe Option(RemoteNode(remoteNodeUriMagnet.uri, appVersion, stateType, 839249))
    }
    client.getPeerInfo[RemotePeer].map { node =>
      node shouldBe Option(RemotePeer(remotePeerUriMagnet.uri, appVersion, stateType, 839249))
    }
  }
  woRestApiAndFullHeight in {
    implicit val testingBackend: SttpBackendStub[Future, _] = SttpBackendStub.asynchronousFuture.whenAnyRequest
      .thenRespondCyclicResponses(
        Response.ok[String](getPeerInfo(woRestApiAndFullHeight)),
        Response.ok[String](getPeerInfo(woRestApiAndFullHeight)),
        Response.ok[String](getPeerInfo(woRestApiAndFullHeight))
      )
    val client = new MetadataHttpClient()
    client.getPeerInfo[LocalNode].map { node =>
      node shouldBe Option(LocalNode(localNodeUriMagnet.uri, appVersion, stateType, 839249))
    }
    client.getPeerInfo[RemoteNode].map { node =>
      node shouldBe Option(RemoteNode(remoteNodeUriMagnet.uri, appVersion, stateType, 839249))
    }
    client.getPeerInfo[RemotePeer].map { node =>
      node shouldBe Option(RemotePeer(remotePeerUriMagnet.uri, appVersion, stateType, 839249))
    }
  }
  woRestApiAndWoFullHeight in {
    implicit val testingBackend: SttpBackendStub[Future, _] = SttpBackendStub.asynchronousFuture.whenAnyRequest
      .thenRespondCyclicResponses(
        Response.ok[String](getPeerInfo(woRestApiAndWoFullHeight)),
        Response.ok[String](getPeerInfo(woRestApiAndWoFullHeight)),
        Response.ok[String](getPeerInfo(woRestApiAndWoFullHeight))
      )
    val client = new MetadataHttpClient()
    client.getPeerInfo[LocalNode].map { node =>
      node shouldBe None
    }
    client.getPeerInfo[RemoteNode].map { node =>
      node shouldBe None
    }
    client.getPeerInfo[RemotePeer].map { node =>
      node shouldBe None
    }
  }

  "client should return only valid connected peers" in {
    implicit val testingBackend: SttpBackendStub[Future, _] = SttpBackendStub.asynchronousFuture.whenAnyRequest
      .thenRespondCyclicResponses(
        Response.ok[String](getConnectedPeers)
      )
    val client = new MetadataHttpClient()

    client.getConnectedPeers(LocalNode(localNodeUriMagnet.uri, appVersion, stateType, 839249)).map { connectedPeers =>
      connectedPeers.collect { case ConnectedPeer(Some(restApiUrl)) => restApiUrl } shouldBe Set(
        uri"https://foo.io"
      )
    }
  }

  "client should handle peerInfo error by returning None" in {
    implicit val testingBackend: SttpBackendStub[Future, _] = SttpBackendStub.asynchronousFuture.whenAnyRequest
      .thenRespondCyclicResponses(
        Response("error", StatusCode.InternalServerError, "Something went wrong")
      )
    val client = new MetadataHttpClient()

    client.getPeerInfo[RemotePeer].futureValue shouldBe None
  }

}
