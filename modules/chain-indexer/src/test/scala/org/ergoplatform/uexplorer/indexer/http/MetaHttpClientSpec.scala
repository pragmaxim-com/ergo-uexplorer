package org.ergoplatform.uexplorer.indexer.http

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import sttp.client3.{Response, _}
import sttp.client3.testing.SttpBackendStub
import sttp.model.StatusCode

import scala.concurrent.Future
import scala.io.Source

class MetaHttpClientSpec extends AsyncFreeSpec with Matchers with BeforeAndAfterAll {

  implicit private val sys: ActorSystem[_] = ActorTestKit().internalSystem

  private val localUri   = uri"http://local"
  private val remoteUri  = uri"http://remote"
  private val restApiUri = uri"http://213.239.193.208:9053"

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

  private implicit val testingBackend: SttpBackendStub[Future, _] = SttpBackendStub.asynchronousFuture.whenAnyRequest
    .thenRespondCyclicResponses(
      Response.ok[String](getPeerInfo(withRestApiAndFullHeight)),
      Response.ok[String](getPeerInfo(woRestApiAndFullHeight)),
      Response.ok[String](getPeerInfo(woRestApiAndWoFullHeight)),
      Response("error", StatusCode.InternalServerError, "Something went wrong")
    )

  private val client = new MetadataHttpClient(localUri, remoteUri)

  withRestApiAndFullHeight in {
    client.getPeerInfo(localUri)(LocalNode.fromPeerInfo).map { node =>
      node shouldBe Option(LocalNode(localUri, 839249, Option(restApiUri)))
    }
  }
  woRestApiAndFullHeight in {
    client.getPeerInfo(localUri)(LocalNode.fromPeerInfo).map { node =>
      node shouldBe Option(LocalNode(localUri, 839249, None))
    }
  }
  woRestApiAndWoFullHeight in {
    client.getPeerInfo(localUri)(LocalNode.fromPeerInfo).map { node =>
      node shouldBe None
    }
  }
}
