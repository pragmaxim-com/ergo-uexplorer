package org.ergoplatform.uexplorer.backend.stats

import org.ergoplatform.uexplorer.backend.ZioRoutes
import org.ergoplatform.uexplorer.backend.blocks.BlockService
import org.ergoplatform.uexplorer.backend.boxes.BoxService
import org.ergoplatform.uexplorer.http.NodePool
import zio.*
import zio.http.*
import zio.json.*
import zio.test.*

trait StatsRoutesSpec extends ZIOSpec[TestEnvironment] with ZioRoutes {

  def statsRoutesSpec(routes: App[Client with NodePool with StatsService with BoxService with BlockService]) =
    suite("StatsRoutesSpec")(
      test("get top-addresses by-value") {
        val path = Root / rootPath / "stats" / "top-addresses" / "by-value"
        val req  = Request.get(url = URL(path).withQueryParams("limit=1"))

        for {
          expectedBody <- routes.runZIO(req).flatMap(_.body.asString)
        } yield assertTrue(1 == expectedBody.split('\n').count(_.nonEmpty))
      },
      test("get top-addresses by-utxo-count") {
        val path = Root / rootPath / "stats" / "top-addresses" / "by-utxo-count"
        val req  = Request.get(url = URL(path).withQueryParams("limit=1"))

        for {
          expectedBody <- routes.runZIO(req).flatMap(_.body.asString)
        } yield assertTrue(0 == expectedBody.split('\n').count(_.nonEmpty))
      }
    )
}
