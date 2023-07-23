package org.ergoplatform.uexplorer.backend.boxes

import eu.timepit.refined.auto.autoUnwrap
import org.ergoplatform.uexplorer.BlockId.unwrapped
import org.ergoplatform.uexplorer.Const.Protocol
import org.ergoplatform.uexplorer.{NetworkPrefix, ProtocolSettings}
import org.ergoplatform.uexplorer.backend.{InmemoryRepo, Repo}
import org.ergoplatform.uexplorer.backend.blocks.{BlockRoutes, InmemoryBlockRepo}
import org.ergoplatform.uexplorer.chain.BlockProcessor
import org.ergoplatform.uexplorer.db.{Block, BlockWithOutputs, LinkedBlock, Utxo}
import org.ergoplatform.uexplorer.http.Rest
import zio.http.*
import zio.json.*
import zio.json.interop.refined.*
import zio.test.*
import zio.*

import java.util.concurrent.TimeUnit

object BoxRoutesSpec extends ZIOSpecDefault {

  private val app                           = BoxRoutes()
  implicit private val ps: ProtocolSettings = ProtocolSettings(NetworkPrefix.fromStringUnsafe("0"))

  def spec = suite("BoxRoutesSpec")(
    test("get box by id") {
      val path = Root / "boxes" / "unspent" / Protocol.firstBlockRewardBox.unwrapped
      val req  = Request.get(url = URL(path))

      for {
        repo         <- ZIO.service[Repo]
        blocks       <- Rest.chain.forHeights(1 to 10)
        _            <- ZIO.collectAllDiscard(blocks.map(b => repo.writeBlock(b)(ZIO.unit, ZIO.unit)))
        expectedBody <- app.runZIO(req).flatMap(_.body.asString)
        actualBox    <- ZIO.fromEither(expectedBody.fromJson[Utxo])
      } yield assertTrue(actualBox.boxId == Protocol.firstBlockRewardBox)
    }.provide(
      InmemoryRepo.layer,
      InmemoryBlockRepo.layer,
      InmemoryBoxRepo.layer
    )
  )
}
