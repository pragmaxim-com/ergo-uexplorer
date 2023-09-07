package org.ergoplatform.uexplorer.backend

import org.ergoplatform.uexplorer.CoreConf
import org.ergoplatform.uexplorer.backend.blocks.{BlockService, PersistentBlockRepo}
import org.ergoplatform.uexplorer.backend.boxes.{BoxRepo, BoxService, PersistentBoxRepo}
import org.ergoplatform.uexplorer.http.{NodePool, NodePoolConf}
import zio.*
import zio.http.*
import zio.json.*
import zio.test.*

trait ProxyRoutesSpec extends ZIOSpec[TestEnvironment] with ZioRoutes {

  def proxyRoutesSpec(routes: App[Client with NodePool with BoxService with BlockService]) = suite("ProxyRoutesSpec")(test("proxy request") {
    val path = Root / "peers" / "status"
    val req  = Request.get(url = URL(path))

    for {
      body   <- routes.runZIO(req).flatMap(_.body.asString)
      status <- ZIO.fromEither(body.fromJson[Status])
    } yield assertTrue(true)
  }).provide(routeLayers)
}

case class Status(lastIncomingMessage: Long, currentSystemTime: Long)
object Status {
  given JsonDecoder[Status] = DeriveJsonDecoder.gen[Status]
  given JsonEncoder[Status] = DeriveJsonEncoder.gen[Status]
}
