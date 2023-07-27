package org.ergoplatform.uexplorer.backend.boxes

import eu.timepit.refined.auto.autoUnwrap
import org.ergoplatform.uexplorer.Const.Protocol
import org.ergoplatform.uexplorer.backend.blocks.InmemoryBlockRepo
import org.ergoplatform.uexplorer.backend.{InmemoryRepo, Repo}
import org.ergoplatform.uexplorer.db.{Box, Utxo}
import org.ergoplatform.uexplorer.http.Rest
import org.ergoplatform.uexplorer.{CoreConf, NetworkPrefix}
import zio.*
import zio.http.*
import zio.json.*
import zio.test.*

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
        repo                    <- ZIO.service[Repo]
        blocks                  <- Rest.chain.forHeights(1 to 10)
        _                       <- ZIO.collectAllDiscard(blocks.map(b => repo.writeBlock(b)(ZIO.unit, ZIO.unit)))
        unspentBox              <- app.runZIO(unspentBoxGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Utxo])))
        unspentBoxes            <- app.runZIO(unspentBoxPost).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Set[Utxo]])))
        spentBox                <- app.runZIO(spentBoxGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Box])))
        anyBox                  <- app.runZIO(anyBoxGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Box])))
        spentBoxes              <- app.runZIO(spentBoxPost).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Set[Box]])))
        anyBoxes                <- app.runZIO(anyBoxPost).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Set[Box]])))
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
    test("get spent/unspent/any box(es) by address / ergo-tree / ergo-tree-hash") {
      val spentByAddressGet   = Request.get(URL(Root / "boxes" / "spent" / "addresses" / Protocol.Emission.address))
      val unspentByAddressGet = Request.get(URL(Root / "boxes" / "unspent" / "addresses" / Protocol.Emission.address))
      val anyByAddressGet     = Request.get(URL(Root / "boxes" / "any" / "addresses" / Protocol.Emission.address))

      val spentByErgoTreeGet       = Request.get(URL(Root / "boxes" / "spent" / "contracts" / "ergo-trees" / Protocol.Emission.ergoTreeHex))
      val unspentByErgoTreeGet     = Request.get(URL(Root / "boxes" / "unspent" / "contracts" / "ergo-trees" / Protocol.Emission.ergoTreeHex))
      val anyByErgoTreeGet         = Request.get(URL(Root / "boxes" / "any" / "contracts" / "ergo-trees" / Protocol.Emission.ergoTreeHex))
      val spentByErgoTreeHashGet   = Request.get(URL(Root / "boxes" / "spent" / "contracts" / "ergo-tree-hashes" / Protocol.Emission.ergoTreeHash))
      val unspentByErgoTreeHashGet = Request.get(URL(Root / "boxes" / "unspent" / "contracts" / "ergo-tree-hashes" / Protocol.Emission.ergoTreeHash))
      val anyByErgoTreeHashGet     = Request.get(URL(Root / "boxes" / "any" / "contracts" / "ergo-tree-hashes" / Protocol.Emission.ergoTreeHash))

      val spentByErgoTreeT8Get       = Request.get(URL(Root / "boxes" / "spent" / "templates" / "ergo-trees" / Protocol.Emission.ergoTreeHex))
      val unspentByErgoTreeT8Get     = Request.get(URL(Root / "boxes" / "unspent" / "templates" / "ergo-trees" / Protocol.Emission.ergoTreeHex))
      val anyByErgoTreeT8Get         = Request.get(URL(Root / "boxes" / "any" / "templates" / "ergo-trees" / Protocol.Emission.ergoTreeHex))
      val spentByErgoTreeHashT8Get   = Request.get(URL(Root / "boxes" / "spent" / "templates" / "ergo-tree-hashes" / Protocol.Emission.ergoTreeHash))
      val unspentByErgoTreeHashT8Get = Request.get(URL(Root / "boxes" / "unspent" / "templates" / "ergo-tree-hashes" / Protocol.Emission.ergoTreeHash))
      val anyByErgoTreeHashT8Get     = Request.get(URL(Root / "boxes" / "any" / "templates" / "ergo-tree-hashes" / Protocol.Emission.ergoTreeHash))

      for {
        repo                    <- ZIO.service[Repo]
        blocks                  <- Rest.chain.forHeights(1 to 10)
        _                       <- ZIO.collectAllDiscard(blocks.map(b => repo.writeBlock(b)(ZIO.unit, ZIO.unit)))
        spentByAddress          <- app.runZIO(spentByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentByAddress        <- app.runZIO(unspentByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyByAddress            <- app.runZIO(anyByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentByErgoTree         <- app.runZIO(spentByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentByErgoTree       <- app.runZIO(unspentByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyByErgoTree           <- app.runZIO(anyByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentByErgoTreeHash     <- app.runZIO(spentByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentByErgoTreeHash   <- app.runZIO(unspentByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyByErgoTreeHash       <- app.runZIO(anyByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentByErgoTreeT8       <- app.runZIO(spentByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentByErgoTreeT8     <- app.runZIO(unspentByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyByErgoTreeT8         <- app.runZIO(anyByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentByErgoTreeT8Hash   <- app.runZIO(spentByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentByErgoTreeT8Hash <- app.runZIO(unspentByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyByErgoTreeT8Hash     <- app.runZIO(anyByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
      } yield assertTrue(
        unspentByAddress.map(_.boxId).intersect(spentByAddress.map(_.boxId)).isEmpty,
        unspentByAddress.map(_.boxId).nonEmpty,
        spentByAddress.map(_.boxId).nonEmpty,
        spentByAddress.size + unspentByAddress.size == anyByAddress.size,
        unspentByErgoTree.map(_.boxId).intersect(spentByErgoTree.map(_.boxId)).isEmpty,
        unspentByErgoTree.map(_.boxId).nonEmpty,
        spentByErgoTree.map(_.boxId).nonEmpty,
        spentByErgoTree.size + unspentByErgoTree.size == anyByErgoTree.size,
        unspentByErgoTreeHash.map(_.boxId).intersect(spentByErgoTreeHash.map(_.boxId)).isEmpty,
        unspentByErgoTreeHash.map(_.boxId).nonEmpty,
        spentByErgoTreeHash.map(_.boxId).nonEmpty,
        spentByErgoTreeHash.size + unspentByErgoTreeHash.size == anyByErgoTreeHash.size,
        // hard to get any real template when we have just first 4000 blocks that have no templates
        unspentByErgoTreeT8.map(_.boxId).intersect(spentByErgoTreeT8.map(_.boxId)).isEmpty,
        unspentByErgoTreeT8.map(_.boxId).isEmpty,
        spentByErgoTreeT8.map(_.boxId).isEmpty,
        spentByErgoTreeT8.size + unspentByErgoTreeT8.size == anyByErgoTreeT8.size,
        unspentByErgoTreeT8Hash.map(_.boxId).intersect(spentByErgoTreeT8Hash.map(_.boxId)).isEmpty,
        unspentByErgoTreeT8Hash.map(_.boxId).isEmpty,
        spentByErgoTreeT8Hash.map(_.boxId).isEmpty,
        spentByErgoTreeT8Hash.size + unspentByErgoTreeT8Hash.size == anyByErgoTreeT8Hash.size
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
