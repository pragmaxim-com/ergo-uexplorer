package org.ergoplatform.uexplorer.backend.boxes

import eu.timepit.refined.auto.autoUnwrap
import org.ergoplatform.uexplorer.BlockId.unwrapped
import org.ergoplatform.uexplorer.Const.Protocol
import org.ergoplatform.uexplorer.{NetworkPrefix, ProtocolSettings}
import org.ergoplatform.uexplorer.backend.{InmemoryRepo, Repo}
import org.ergoplatform.uexplorer.backend.blocks.{BlockRoutes, InmemoryBlockRepo}
import org.ergoplatform.uexplorer.chain.BlockProcessor
import org.ergoplatform.uexplorer.db.{Block, BlockWithOutputs, Box, LinkedBlock, Utxo}
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
    test("get spent/unspent box(es) by id") {
      val unspentBoxGet        = Request.get(URL(Root / "boxes" / "unspent" / Protocol.firstBlockRewardBox.unwrapped))
      val missingUnspentBoxGet = Request.get(URL(Root / "boxes" / "unspent" / Protocol.Emission.outputBox.unwrapped))
      val spentBoxGet          = Request.get(URL(Root / "boxes" / "spent" / Protocol.Emission.outputBox.unwrapped))
      val missingSpentBoxGet   = Request.get(URL(Root / "boxes" / "spent" / Protocol.firstBlockRewardBox.unwrapped))

      val unspentBoxPost =
        Request.post(
          Body.fromString(Set(Protocol.firstBlockRewardBox, Protocol.Emission.outputBox).toJson),
          URL(Root / "boxes" / "unspent")
        )
      val spentBoxPost =
        Request.post(
          Body.fromString(Set(Protocol.Emission.outputBox, Protocol.firstBlockRewardBox).toJson),
          URL(Root / "boxes" / "spent")
        )

      for {
        repo         <- ZIO.service[Repo]
        blocks       <- Rest.chain.forHeights(1 to 10)
        s            <- ZIO.collectAllDiscard(blocks.map(b => repo.writeBlock(b)(ZIO.unit, ZIO.unit)))
        unspentBox   <- app.runZIO(unspentBoxGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Utxo])))
        unspentBoxes <- app.runZIO(unspentBoxPost).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Set[Utxo]])))
        spentBox     <- app.runZIO(spentBoxGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Box])))
        spentBoxes   <- app.runZIO(spentBoxPost).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Set[Box]])))
        missingSpentBoxStatus   <- app.runZIO(missingSpentBoxGet).map(_.status)
        missingUnspentBoxStatus <- app.runZIO(missingUnspentBoxGet).map(_.status)
      } yield assertTrue(
        unspentBox.boxId == Protocol.firstBlockRewardBox,
        unspentBoxes.map(_.boxId) == Set(Protocol.firstBlockRewardBox),
        spentBox.boxId == Protocol.Emission.outputBox,
        spentBoxes.map(_.boxId) == Set(Protocol.Emission.outputBox),
        missingSpentBoxStatus == Status.NotFound,
        missingUnspentBoxStatus == Status.NotFound
      )
    }.provide(
      InmemoryRepo.layer,
      InmemoryBlockRepo.layer,
      InmemoryBoxRepo.layer
    )
  )
}
