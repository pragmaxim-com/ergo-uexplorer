package org.ergoplatform.uexplorer.backend.blocks

import org.ergoplatform.uexplorer.Const.Protocol
import org.ergoplatform.uexplorer.backend.blocks.BlockRoutes
import zio.*
import zio.http.*
import zio.json.*
import zio.{Duration, Scope, URIO, ZLayer}
import zio.test.*
import eu.timepit.refined.auto.autoUnwrap
import zio.json.interop.refined.*
import org.ergoplatform.uexplorer.BlockId.unwrapped
import org.ergoplatform.uexplorer.backend.blocks.Info
import org.ergoplatform.uexplorer.backend.boxes.PersistentBoxRepo
import org.ergoplatform.uexplorer.{CoreConf, NetworkPrefix}
import org.ergoplatform.uexplorer.backend.{H2Backend, PersistentRepo, Repo}
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.http.Rest

import java.util.concurrent.TimeUnit

trait BlockRoutesSpec extends ZIOSpec[TestEnvironment] {

  private val blockRoutes = BlockRoutes()

  def blockRoutesSpec = suite("BlockRoutesSpec")(
    test("get info") {
      val path = Root / "info"
      val req  = Request.get(url = URL(path))

      for {
        expectedBody <- blockRoutes.runZIO(req).flatMap(_.body.asString)
        expectedInfo <- ZIO.fromEither(expectedBody.fromJson[Info])
      } yield assertTrue(10 == expectedInfo.lastHeight)
    }.provide(
      H2Backend.layer,
      PersistentBlockRepo.layer
    ),
    test("get block by id") {
      val path = Root / "blocks" / Protocol.firstBlockId.unwrapped
      val req  = Request.get(url = URL(path))

      for {
        expectedBody  <- blockRoutes.runZIO(req).flatMap(_.body.asString)
        expectedBlock <- ZIO.fromEither(expectedBody.fromJson[Block])
      } yield assertTrue(1 == expectedBlock.height)
    }.provide(
      H2Backend.layer,
      PersistentBlockRepo.layer
    ),
    test("get blocks by ids") {
      val path = Root / "blocks"
      val req  = Request.post(url = URL(path), body = Body.fromString(List(Protocol.firstBlockId).toJson))

      for {
        expectedBody   <- blockRoutes.runZIO(req).flatMap(_.body.asString)
        expectedBlocks <- ZIO.fromEither(expectedBody.fromJson[List[Block]])
      } yield assertTrue(List(1) == expectedBlocks.map(_.height))
    }.provide(
      H2Backend.layer,
      PersistentBlockRepo.layer
    )
  )
}
