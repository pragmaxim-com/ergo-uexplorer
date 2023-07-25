package org.ergoplatform.uexplorer.backend.boxes

import eu.timepit.refined.auto.autoUnwrap
import org.ergoplatform.uexplorer.BlockId.unwrapped
import org.ergoplatform.uexplorer.Const.Protocol
import org.ergoplatform.uexplorer.{CoreConf, NetworkPrefix}
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

  private val app                   = BoxRoutes()
  implicit private val ps: CoreConf = CoreConf(NetworkPrefix.fromStringUnsafe("0"))

  def spec = suite("BoxRoutesSpec")(
    test("get spent/unspent/any box(es) by id") {
      val unspentBoxGet        = Request.get(URL(Root / "boxes" / "unspent" / Protocol.firstBlockRewardBox.unwrapped))
      val missingUnspentBoxGet = Request.get(URL(Root / "boxes" / "unspent" / Protocol.Emission.outputBox.unwrapped))
      val spentBoxGet          = Request.get(URL(Root / "boxes" / "spent" / Protocol.Emission.outputBox.unwrapped))
      val missingSpentBoxGet   = Request.get(URL(Root / "boxes" / "spent" / Protocol.firstBlockRewardBox.unwrapped))
      val anyBoxGet            = Request.get(URL(Root / "boxes" / "any" / Protocol.Emission.outputBox.unwrapped))

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
      val anyBoxPost =
        Request.post(
          Body.fromString(Set(Protocol.Emission.outputBox, Protocol.firstBlockRewardBox).toJson),
          URL(Root / "boxes" / "any")
        )

      for {
        repo         <- ZIO.service[Repo]
        blocks       <- Rest.chain.forHeights(1 to 10)
        _            <- ZIO.collectAllDiscard(blocks.map(b => repo.writeBlock(b)(ZIO.unit, ZIO.unit)))
        unspentBox   <- app.runZIO(unspentBoxGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Utxo])))
        unspentBoxes <- app.runZIO(unspentBoxPost).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Set[Utxo]])))
        spentBox     <- app.runZIO(spentBoxGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Box])))
        anyBox       <- app.runZIO(anyBoxGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Box])))
        spentBoxes   <- app.runZIO(spentBoxPost).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Set[Box]])))
        anyBoxes     <- app.runZIO(anyBoxPost).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Set[Box]])))
        missingSpentBoxStatus   <- app.runZIO(missingSpentBoxGet).map(_.status)
        missingUnspentBoxStatus <- app.runZIO(missingUnspentBoxGet).map(_.status)
      } yield assertTrue(
        unspentBox.boxId == Protocol.firstBlockRewardBox,
        unspentBoxes.map(_.boxId) == Set(Protocol.firstBlockRewardBox),
        spentBox.boxId == Protocol.Emission.outputBox,
        anyBox.boxId == Protocol.Emission.outputBox,
        spentBoxes.map(_.boxId) == Set(Protocol.Emission.outputBox),
        anyBoxes.map(_.boxId) == Set(Protocol.Emission.outputBox, Protocol.firstBlockRewardBox),
        missingSpentBoxStatus == Status.NotFound,
        missingUnspentBoxStatus == Status.NotFound
      )
    }.provide(
      InmemoryRepo.layer,
      InmemoryBlockRepo.layer,
      InmemoryBoxRepo.layer,
      BoxService.layer,
      CoreConf.layer
    ),
    test("get spent/unspent/any box(es) by address") {
      val spentBoxesByAddressGet   = Request.get(URL(Root / "boxes" / "spent" / "addresses" / Protocol.Emission.address))
      val unspentBoxesByAddressGet = Request.get(URL(Root / "boxes" / "unspent" / "addresses" / Protocol.Emission.address))
      val anyBoxesByAddressGet     = Request.get(URL(Root / "boxes" / "any" / "addresses" / Protocol.Emission.address))

      for {
        repo   <- ZIO.service[Repo]
        blocks <- Rest.chain.forHeights(1 to 10)
        _      <- ZIO.collectAllDiscard(blocks.map(b => repo.writeBlock(b)(ZIO.unit, ZIO.unit)))
        spentBoxes <-
          app.runZIO(spentBoxesByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentBoxes <-
          app.runZIO(unspentBoxesByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyBoxes <-
          app.runZIO(anyBoxesByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
      } yield assertTrue(
        unspentBoxes.map(_.boxId).intersect(spentBoxes.map(_.boxId)).isEmpty,
        unspentBoxes.map(_.boxId).nonEmpty,
        spentBoxes.map(_.boxId).nonEmpty,
        spentBoxes.size + unspentBoxes.size == anyBoxes.size
      )
    }.provide(
      InmemoryRepo.layer,
      InmemoryBlockRepo.layer,
      InmemoryBoxRepo.layer,
      BoxService.layer,
      CoreConf.layer
    )
  )
}
