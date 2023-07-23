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
import org.ergoplatform.uexplorer.db.Block

import java.util.concurrent.TimeUnit

object BlockRoutesSpec extends ZIOSpecDefault {

  private val app = BlockRoutes()

  def spec = suite("BlockRoutesSpec")(
    test("get block by id") {
      val path = Root / "blocks" / Protocol.genesisBlockId.unwrapped
      val req  = Request.get(url = URL(path))

      for {
        expectedBody <- app.runZIO(req).flatMap(_.body.asString)
      } yield assertTrue(new Block().toJson == expectedBody)
    }.provide(
      InmemoryBlockRepo.layerWithBlocks(List(new Block()))
    ),
    test("get blocks by ids") {
      val path = Root / "blocks"
      val req  = Request.post(url = URL(path), body = Body.fromString(List(Protocol.genesisBlockId).toJson))

      for {
        expectedBody <- app.runZIO(req).flatMap(_.body.asString)
      } yield assertTrue(List(new Block()).toJson == expectedBody)
    }.provide(
      InmemoryBlockRepo.layerWithBlocks(List(new Block()))
    )
  )
}
