package org.ergoplatform.uexplorer.http

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorSystem
import org.ergoplatform.uexplorer.http.{Rest, TestSupport}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import sttp.client3.testing.SttpBackendStub
import sttp.client3._
import sttp.model.StatusCode

import scala.concurrent.Future
import org.ergoplatform.uexplorer.http.LocalNodeUriMagnet
import org.ergoplatform.uexplorer.http.RemoteNodeUriMagnet
import org.ergoplatform.uexplorer.http.MetadataHttpClient
import org.ergoplatform.uexplorer.http.RemoteNode
import org.ergoplatform.uexplorer.http.RemotePeer
import org.ergoplatform.uexplorer.http.LocalNode
import org.ergoplatform.uexplorer.http.RemotePeerUriMagnet
import org.ergoplatform.uexplorer.http.ConnectedPeer

class MetaHttpClientSpec extends AsyncFreeSpec with TestSupport with Matchers with BeforeAndAfterAll with ScalaFutures {

  implicit private val sys: ActorSystem[_] = ActorTestKit().internalSystem

  implicit private val localNodeUriMagnet: LocalNodeUriMagnet   = LocalNodeUriMagnet(uri"http://local")
  implicit private val remoteNodeUriMagnet: RemoteNodeUriMagnet = RemoteNodeUriMagnet(uri"http://remote")
  implicit private val remotePeerUriMagnet: RemotePeerUriMagnet = RemotePeerUriMagnet(uri"http://peer")

  private val appVersion = "4.0.42"
  private val stateType  = "utxo"

  Rest.info.sync in {
    implicit val testingBackend: SttpBackendStub[Future, _] = SttpBackendStub.asynchronousFuture.whenAnyRequest
      .thenRespondCyclicResponses(
        Response.ok[String](getPeerInfo(Rest.info.sync)),
        Response.ok[String](getPeerInfo(Rest.info.sync)),
        Response.ok[String](getPeerInfo(Rest.info.sync))
      )
    val client = new MetadataHttpClient(Rest.info.minNodeHeight)
    client.getPeerInfo[LocalNode]().map { node =>
      node shouldBe Option(LocalNode(localNodeUriMagnet.uri, appVersion, stateType, 839249))
    }
    client.getPeerInfo[RemoteNode]().map { node =>
      node shouldBe Option(RemoteNode(remoteNodeUriMagnet.uri, appVersion, stateType, 839249))
    }
    client.getPeerInfo[RemotePeer]().map { node =>
      node shouldBe Option(RemotePeer(remotePeerUriMagnet.uri, appVersion, stateType, 4150))
    }
  }
  Rest.info.woRestApiAndFullHeight in {
    implicit val testingBackend: SttpBackendStub[Future, _] = SttpBackendStub.asynchronousFuture.whenAnyRequest
      .thenRespondCyclicResponses(
        Response.ok[String](getPeerInfo(Rest.info.woRestApiAndFullHeight)),
        Response.ok[String](getPeerInfo(Rest.info.woRestApiAndFullHeight)),
        Response.ok[String](getPeerInfo(Rest.info.woRestApiAndFullHeight))
      )
    val client = new MetadataHttpClient(Rest.info.minNodeHeight)
    client.getPeerInfo[LocalNode]().map { node =>
      node shouldBe Option(LocalNode(localNodeUriMagnet.uri, appVersion, stateType, 839249))
    }
    client.getPeerInfo[RemoteNode]().map { node =>
      node shouldBe Option(RemoteNode(remoteNodeUriMagnet.uri, appVersion, stateType, 839249))
    }
    client.getPeerInfo[RemotePeer]().map { node =>
      node shouldBe Option(RemotePeer(remotePeerUriMagnet.uri, appVersion, stateType, 4200))
    }
  }
  Rest.info.woRestApiAndWoFullHeight in {
    implicit val testingBackend: SttpBackendStub[Future, _] = SttpBackendStub.asynchronousFuture.whenAnyRequest
      .thenRespondCyclicResponses(
        Response.ok[String](getPeerInfo(Rest.info.woRestApiAndWoFullHeight)),
        Response.ok[String](getPeerInfo(Rest.info.woRestApiAndWoFullHeight)),
        Response.ok[String](getPeerInfo(Rest.info.woRestApiAndWoFullHeight))
      )
    val client = new MetadataHttpClient(Rest.info.minNodeHeight)
    client.getPeerInfo[LocalNode]().map { node =>
      node shouldBe None
    }
    client.getPeerInfo[RemoteNode]().map { node =>
      node shouldBe None
    }
    client.getPeerInfo[RemotePeer]().map { node =>
      node shouldBe None
    }
  }

  "client should return only valid connected peers" in {
    implicit val testingBackend: SttpBackendStub[Future, _] = SttpBackendStub.asynchronousFuture.whenAnyRequest
      .thenRespondCyclicResponses(
        Response.ok[String](getConnectedPeers)
      )
    val client = new MetadataHttpClient(Rest.info.minNodeHeight)

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
    val client = new MetadataHttpClient(Rest.info.minNodeHeight)

    client.getPeerInfo[RemotePeer]().futureValue shouldBe None
  }

}
