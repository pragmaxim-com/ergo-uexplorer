package org.ergoplatform.uexplorer.backend.boxes

import eu.timepit.refined.auto.autoUnwrap
import org.ergoplatform.uexplorer.Const.Protocol
import org.ergoplatform.uexplorer.Const.Protocol.Emission
import org.ergoplatform.uexplorer.backend.blocks.PersistentBlockRepo
import org.ergoplatform.uexplorer.backend.{H2Backend, PersistentRepo, Repo}
import org.ergoplatform.uexplorer.db.{Asset, Box, Utxo}
import org.ergoplatform.uexplorer.http.Rest
import org.ergoplatform.uexplorer.{BoxId, CoreConf, NetworkPrefix}
import zio.*
import zio.http.*
import zio.json.*
import zio.test.*

import scala.collection.immutable.Set

object BoxRoutesSpec extends ZIOSpecDefault {

  private val app                                     = BoxRoutes()
  private val indexFilter: Map[String, Chunk[String]] = BoxService.indexWhiteList.map(key => key -> Chunk("")).toMap
  implicit private val ps: CoreConf                   = CoreConf(NetworkPrefix.fromStringUnsafe("0"))

  def spec = suite("BoxRoutesSpec")(
    test("get spent/unspent/any box(es) by id") {
      val unspentBoxGet        = Request.get(URL(Root / "boxes" / "unspent" / Protocol.firstBlockRewardBox.unwrapped))
      val missingUnspentBoxGet = Request.get(URL(Root / "boxes" / "unspent" / Emission.outputBox.unwrapped))
      val spentBoxGet          = Request.get(URL(Root / "boxes" / "spent" / Emission.outputBox.unwrapped))
      val missingSpentBoxGet   = Request.get(URL(Root / "boxes" / "spent" / Protocol.firstBlockRewardBox.unwrapped))
      val anyBoxGet            = Request.get(URL(Root / "boxes" / "any" / Emission.outputBox.unwrapped))

      val unspentBoxPost =
        Request.post(
          Body.fromString(Set(Protocol.firstBlockRewardBox, Emission.outputBox).toJson),
          URL(Root / "boxes" / "unspent")
        )
      val spentBoxPost =
        Request.post(
          Body.fromString(Set(Emission.outputBox, Protocol.firstBlockRewardBox).toJson),
          URL(Root / "boxes" / "spent")
        )
      val anyBoxPost =
        Request.post(
          Body.fromString(Set(Emission.outputBox, Protocol.firstBlockRewardBox).toJson),
          URL(Root / "boxes" / "any")
        )

      for {
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
        spentBox.boxId == Emission.outputBox,
        anyBox.boxId == Emission.outputBox,
        spentBoxes.map(_.boxId) == Set(Emission.outputBox),
        anyBoxes.map(_.boxId) == Set(Emission.outputBox, Protocol.firstBlockRewardBox),
        missingSpentBoxStatus == Status.NotFound,
        missingUnspentBoxStatus == Status.NotFound
      )
    }.provide(
      H2Backend.layer,
      PersistentBoxRepo.layer,
      BoxService.layer,
      CoreConf.layer
    ),
    test("get spent/unspent/any box(es) by address") {
      val spentBoxesByAddressGet   = Request.get(URL(Root / "boxes" / "spent" / "by-address" / Emission.address))
      val unspentBoxesByAddressGet = Request.get(URL(Root / "boxes" / "unspent" / "by-address" / Emission.address))
      val anyBoxesByAddressGet     = Request.get(URL(Root / "boxes" / "any" / "by-address" / Emission.address))

      val spentBoxIdsByAddressGet   = Request.get(URL(Root / "box-ids" / "spent" / "by-address" / Emission.address))
      val unspentBoxIdsByAddressGet = Request.get(URL(Root / "box-ids" / "unspent" / "by-address" / Emission.address))
      val anyBoxIdsByAddressGet     = Request.get(URL(Root / "box-ids" / "any" / "by-address" / Emission.address))

      for {
        spentBoxesByAddress    <- app.runZIO(spentBoxesByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentBoxesByAddress  <- app.runZIO(unspentBoxesByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyBoxesByAddress      <- app.runZIO(anyBoxesByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentBoxIdsByAddress   <- app.runZIO(spentBoxIdsByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentBoxIdsByAddress <- app.runZIO(unspentBoxIdsByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyBoxIdsByAddress     <- app.runZIO(anyBoxIdsByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
      } yield assertTrue(
        unspentBoxesByAddress.map(_.boxId).intersect(spentBoxesByAddress.map(_.boxId)).isEmpty,
        unspentBoxesByAddress.map(_.boxId).nonEmpty,
        spentBoxesByAddress.map(_.boxId).nonEmpty,
        spentBoxesByAddress.size + unspentBoxesByAddress.size == anyBoxesByAddress.size,
        unspentBoxIdsByAddress.intersect(spentBoxIdsByAddress).isEmpty,
        unspentBoxIdsByAddress.nonEmpty,
        spentBoxIdsByAddress.nonEmpty,
        spentBoxIdsByAddress.size + unspentBoxIdsByAddress.size == anyBoxIdsByAddress.size
      )
    }.provide(
      H2Backend.layer,
      PersistentBoxRepo.layer,
      BoxService.layer,
      CoreConf.layer
    ),
    test("get spent/unspent/any box contracts by ergo-tree") {
      val spentBoxesByErgoTreeGet   = Request.get(URL(Root / "boxes" / "spent" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex))
      val unspentBoxesByErgoTreeGet = Request.get(URL(Root / "boxes" / "unspent" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex))
      val anyBoxesByErgoTreeGet     = Request.get(URL(Root / "boxes" / "any" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex))

      val spentBoxIdsByErgoTreeGet   = Request.get(URL(Root / "box-ids" / "spent" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex))
      val unspentBoxIdsByErgoTreeGet = Request.get(URL(Root / "box-ids" / "unspent" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex))
      val anyBoxIdsByErgoTreeGet     = Request.get(URL(Root / "box-ids" / "any" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex))

      for {
        spentBoxesByErgoTree    <- app.runZIO(spentBoxesByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentBoxesByErgoTree  <- app.runZIO(unspentBoxesByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyBoxesByErgoTree      <- app.runZIO(anyBoxesByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentBoxIdsByErgoTree   <- app.runZIO(spentBoxIdsByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentBoxIdsByErgoTree <- app.runZIO(unspentBoxIdsByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyBoxIdsByErgoTree     <- app.runZIO(anyBoxIdsByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
      } yield assertTrue(
        unspentBoxesByErgoTree.map(_.boxId).intersect(spentBoxesByErgoTree.map(_.boxId)).isEmpty,
        unspentBoxesByErgoTree.map(_.boxId).nonEmpty,
        spentBoxesByErgoTree.map(_.boxId).nonEmpty,
        spentBoxesByErgoTree.size + unspentBoxesByErgoTree.size == anyBoxesByErgoTree.size,
        unspentBoxIdsByErgoTree.intersect(spentBoxIdsByErgoTree).isEmpty,
        unspentBoxIdsByErgoTree.nonEmpty,
        spentBoxIdsByErgoTree.nonEmpty,
        spentBoxIdsByErgoTree.size + unspentBoxIdsByErgoTree.size == anyBoxIdsByErgoTree.size
      )
    }.provide(
      H2Backend.layer,
      PersistentBoxRepo.layer,
      BoxService.layer,
      CoreConf.layer
    ),
    test("get spent/unspent/any box templates by ergo-tree") {
      val spentBoxesByErgoTreeT8Get   = Request.get(URL(Root / "boxes" / "spent" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex))
      val unspentBoxesByErgoTreeT8Get = Request.get(URL(Root / "boxes" / "unspent" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex))
      val anyBoxesByErgoTreeT8Get     = Request.get(URL(Root / "boxes" / "any" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex))

      val spentBoxIdsByErgoTreeT8Get   = Request.get(URL(Root / "box-ids" / "spent" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex))
      val unspentBoxIdsByErgoTreeT8Get = Request.get(URL(Root / "box-ids" / "unspent" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex))
      val anyBoxIdsByErgoTreeT8Get     = Request.get(URL(Root / "box-ids" / "any" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex))
      for {
        spentBoxesByErgoTreeT8    <- app.runZIO(spentBoxesByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentBoxesByErgoTreeT8  <- app.runZIO(unspentBoxesByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyBoxesByErgoTreeT8      <- app.runZIO(anyBoxesByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentBoxIdsByErgoTreeT8   <- app.runZIO(spentBoxIdsByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentBoxIdsByErgoTreeT8 <- app.runZIO(unspentBoxIdsByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyBoxIdsByErgoTreeT8     <- app.runZIO(anyBoxIdsByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
      } yield assertTrue(
        // hard to get any real template when we have just first 4000 blocks that have no templates
        unspentBoxesByErgoTreeT8.map(_.boxId).intersect(spentBoxesByErgoTreeT8.map(_.boxId)).isEmpty,
        unspentBoxesByErgoTreeT8.map(_.boxId).isEmpty,
        spentBoxesByErgoTreeT8.map(_.boxId).isEmpty,
        spentBoxesByErgoTreeT8.size + unspentBoxesByErgoTreeT8.size == anyBoxesByErgoTreeT8.size,
        unspentBoxIdsByErgoTreeT8.intersect(spentBoxIdsByErgoTreeT8).isEmpty,
        unspentBoxIdsByErgoTreeT8.isEmpty,
        spentBoxIdsByErgoTreeT8.isEmpty,
        spentBoxIdsByErgoTreeT8.size + unspentBoxIdsByErgoTreeT8.size == anyBoxIdsByErgoTreeT8.size
      )
    }.provide(
      H2Backend.layer,
      PersistentBoxRepo.layer,
      BoxService.layer,
      CoreConf.layer
    ),
    test("get spent/unspent/any box contracts by ergo-tree-hash") {
      val spentBoxesByErgoTreeHashGet   = Request.get(URL(Root / "boxes" / "spent" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash))
      val unspentBoxesByErgoTreeHashGet = Request.get(URL(Root / "boxes" / "unspent" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash))
      val anyBoxesByErgoTreeHashGet     = Request.get(URL(Root / "boxes" / "any" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash))

      val spentBoxIdsByErgoTreeHashGet   = Request.get(URL(Root / "box-ids" / "spent" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash))
      val unspentBoxIdsByErgoTreeHashGet = Request.get(URL(Root / "box-ids" / "unspent" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash))
      val anyBoxIdsByErgoTreeHashGet     = Request.get(URL(Root / "box-ids" / "any" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash))

      for {
        spentBoxesByErgoTreeHash    <- app.runZIO(spentBoxesByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentBoxesByErgoTreeHash  <- app.runZIO(unspentBoxesByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyBoxesByErgoTreeHash      <- app.runZIO(anyBoxesByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentBoxIdsByErgoTreeHash   <- app.runZIO(spentBoxIdsByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentBoxIdsByErgoTreeHash <- app.runZIO(unspentBoxIdsByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyBoxIdsByErgoTreeHash     <- app.runZIO(anyBoxIdsByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
      } yield assertTrue(
        unspentBoxesByErgoTreeHash.map(_.boxId).intersect(spentBoxesByErgoTreeHash.map(_.boxId)).isEmpty,
        unspentBoxesByErgoTreeHash.map(_.boxId).nonEmpty,
        spentBoxesByErgoTreeHash.map(_.boxId).nonEmpty,
        spentBoxesByErgoTreeHash.size + unspentBoxesByErgoTreeHash.size == anyBoxesByErgoTreeHash.size,
        unspentBoxIdsByErgoTreeHash.intersect(spentBoxIdsByErgoTreeHash).isEmpty,
        unspentBoxIdsByErgoTreeHash.nonEmpty,
        spentBoxIdsByErgoTreeHash.nonEmpty,
        spentBoxIdsByErgoTreeHash.size + unspentBoxIdsByErgoTreeHash.size == anyBoxesByErgoTreeHash.size
      )
    }.provide(
      H2Backend.layer,
      PersistentBoxRepo.layer,
      BoxService.layer,
      CoreConf.layer
    ),
    test("get spent/unspent/any box templates by ergo-tree-hash") {
      val spentByErgoTreeHashT8Get   = Request.get(URL(Root / "boxes" / "spent" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash))
      val unspentByErgoTreeHashT8Get = Request.get(URL(Root / "boxes" / "unspent" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash))
      val anyByErgoTreeHashT8Get     = Request.get(URL(Root / "boxes" / "any" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash))

      val spentIdsByErgoTreeHashT8Get   = Request.get(URL(Root / "box-ids" / "spent" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash))
      val unspentIdsByErgoTreeHashT8Get = Request.get(URL(Root / "box-ids" / "unspent" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash))
      val anyIdsByErgoTreeHashT8Get     = Request.get(URL(Root / "box-ids" / "any" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash))

      for {
        spentByErgoTreeT8Hash      <- app.runZIO(spentByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentByErgoTreeT8Hash    <- app.runZIO(unspentByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyByErgoTreeT8Hash        <- app.runZIO(anyByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentIdsByErgoTreeT8Hash   <- app.runZIO(spentIdsByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentIdsByErgoTreeT8Hash <- app.runZIO(unspentIdsByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyIdsByErgoTreeT8Hash     <- app.runZIO(anyIdsByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
      } yield assertTrue(
        unspentByErgoTreeT8Hash.map(_.boxId).intersect(spentByErgoTreeT8Hash.map(_.boxId)).isEmpty,
        unspentByErgoTreeT8Hash.map(_.boxId).isEmpty,
        spentByErgoTreeT8Hash.map(_.boxId).isEmpty,
        spentByErgoTreeT8Hash.size + unspentByErgoTreeT8Hash.size == anyByErgoTreeT8Hash.size,
        unspentIdsByErgoTreeT8Hash.intersect(spentIdsByErgoTreeT8Hash).isEmpty,
        unspentIdsByErgoTreeT8Hash.isEmpty,
        spentIdsByErgoTreeT8Hash.isEmpty,
        spentIdsByErgoTreeT8Hash.size + unspentIdsByErgoTreeT8Hash.size == anyIdsByErgoTreeT8Hash.size
      )
    }.provide(
      H2Backend.layer,
      PersistentBoxRepo.layer,
      BoxService.layer,
      CoreConf.layer
    ),
    test("get spent/unspent/any box(es) by address with index filter") {
      val spentByAddressGet   = Request.get(URL(Root / "boxes" / "spent" / "by-address" / Emission.address).withQueryParams(indexFilter))
      val unspentByAddressGet = Request.get(URL(Root / "boxes" / "unspent" / "by-address" / Emission.address).withQueryParams(indexFilter))
      val anyByAddressGet     = Request.get(URL(Root / "boxes" / "any" / "by-address" / Emission.address).withQueryParams(indexFilter))

      val spentIdsByAddressGet   = Request.get(URL(Root / "box-ids" / "spent" / "by-address" / Emission.address).withQueryParams(indexFilter))
      val unspentIdsByAddressGet = Request.get(URL(Root / "box-ids" / "unspent" / "by-address" / Emission.address).withQueryParams(indexFilter))
      val anyIdsByAddressGet     = Request.get(URL(Root / "box-ids" / "any" / "by-address" / Emission.address).withQueryParams(indexFilter))

      for {
        spentByAddress      <- app.runZIO(spentByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentByAddress    <- app.runZIO(unspentByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyByAddress        <- app.runZIO(anyByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentIdsByAddress   <- app.runZIO(spentIdsByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentIdsByAddress <- app.runZIO(unspentIdsByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyIdsByAddress     <- app.runZIO(anyIdsByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
      } yield assertTrue(
        unspentByAddress.map(_.boxId).intersect(spentByAddress.map(_.boxId)).isEmpty,
        unspentByAddress.map(_.boxId).isEmpty,
        spentByAddress.map(_.boxId).isEmpty,
        spentByAddress.size + unspentByAddress.size == anyByAddress.size,
        unspentIdsByAddress.intersect(spentIdsByAddress).isEmpty,
        unspentIdsByAddress.isEmpty,
        spentIdsByAddress.isEmpty,
        spentIdsByAddress.size + unspentIdsByAddress.size == anyIdsByAddress.size
      )
    }.provide(
      H2Backend.layer,
      PersistentBoxRepo.layer,
      BoxService.layer,
      CoreConf.layer
    ),
    test("get spent/unspent/any box contracts by ergo-tree with index filter") {
      val spentByErgoTreeGet   = Request.get(URL(Root / "boxes" / "spent" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))
      val unspentByErgoTreeGet = Request.get(URL(Root / "boxes" / "unspent" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))
      val anyByErgoTreeGet     = Request.get(URL(Root / "boxes" / "any" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))

      val spentIdsByErgoTreeGet =
        Request.get(URL(Root / "box-ids" / "spent" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))
      val unspentIdsByErgoTreeGet =
        Request.get(URL(Root / "box-ids" / "unspent" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))
      val anyIdsByErgoTreeGet = Request.get(URL(Root / "boxes" / "any" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))

      for {
        spentByErgoTree      <- app.runZIO(spentByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentByErgoTree    <- app.runZIO(unspentByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyByErgoTree        <- app.runZIO(anyByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentIdsByErgoTree   <- app.runZIO(spentIdsByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentIdsByErgoTree <- app.runZIO(unspentIdsByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyIdsByErgoTree     <- app.runZIO(anyIdsByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
      } yield assertTrue(
        unspentByErgoTree.map(_.boxId).intersect(spentByErgoTree.map(_.boxId)).isEmpty,
        unspentByErgoTree.map(_.boxId).isEmpty,
        spentByErgoTree.map(_.boxId).isEmpty,
        spentByErgoTree.size + unspentByErgoTree.size == anyByErgoTree.size,
        unspentIdsByErgoTree.intersect(spentIdsByErgoTree).isEmpty,
        unspentIdsByErgoTree.isEmpty,
        spentIdsByErgoTree.isEmpty,
        spentIdsByErgoTree.size + unspentIdsByErgoTree.size == anyIdsByErgoTree.size
      )
    }.provide(
      H2Backend.layer,
      PersistentBoxRepo.layer,
      BoxService.layer,
      CoreConf.layer
    ),
    test("get spent/unspent/any box templates by ergo-tree with index filter") {

      val spentByErgoTreeT8Get = Request.get(URL(Root / "boxes" / "spent" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))
      val unspentByErgoTreeT8Get =
        Request.get(URL(Root / "boxes" / "unspent" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))
      val anyByErgoTreeT8Get = Request.get(URL(Root / "boxes" / "any" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))

      val spentIdsByErgoTreeT8Get =
        Request.get(URL(Root / "box-ids" / "spent" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))
      val unspentIdsByErgoTreeT8Get =
        Request.get(URL(Root / "box-ids" / "unspent" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))
      val anyIdsByErgoTreeT8Get = Request.get(URL(Root / "box-ids" / "any" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))

      for {
        spentByErgoTreeT8      <- app.runZIO(spentByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentByErgoTreeT8    <- app.runZIO(unspentByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyByErgoTreeT8        <- app.runZIO(anyByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentIdsByErgoTreeT8   <- app.runZIO(spentIdsByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentIdsByErgoTreeT8 <- app.runZIO(unspentIdsByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyIdsByErgoTreeT8     <- app.runZIO(anyIdsByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
      } yield assertTrue(
        unspentByErgoTreeT8.map(_.boxId).intersect(spentByErgoTreeT8.map(_.boxId)).isEmpty,
        unspentByErgoTreeT8.map(_.boxId).isEmpty,
        spentByErgoTreeT8.map(_.boxId).isEmpty,
        spentByErgoTreeT8.size + unspentByErgoTreeT8.size == anyByErgoTreeT8.size,
        unspentIdsByErgoTreeT8.intersect(spentIdsByErgoTreeT8).isEmpty,
        unspentIdsByErgoTreeT8.isEmpty,
        spentIdsByErgoTreeT8.isEmpty,
        spentIdsByErgoTreeT8.size + unspentIdsByErgoTreeT8.size == anyIdsByErgoTreeT8.size
      )
    }.provide(
      H2Backend.layer,
      PersistentBoxRepo.layer,
      BoxService.layer,
      CoreConf.layer
    ),
    test("get spent/unspent/any box contracts by ergo-tree-hash with index filter") {
      val spentByErgoTreeHashGet =
        Request.get(URL(Root / "boxes" / "spent" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))
      val unspentByErgoTreeHashGet =
        Request.get(URL(Root / "boxes" / "unspent" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))
      val anyByErgoTreeHashGet =
        Request.get(URL(Root / "boxes" / "any" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))

      val spentIdsByErgoTreeHashGet =
        Request.get(URL(Root / "box-ids" / "spent" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))
      val unspentIdsByErgoTreeHashGet =
        Request.get(URL(Root / "box-ids" / "unspent" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))
      val anyIdsByErgoTreeHashGet =
        Request.get(URL(Root / "box-ids" / "any" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))

      for {
        spentByErgoTreeHash      <- app.runZIO(spentByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentByErgoTreeHash    <- app.runZIO(unspentByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyByErgoTreeHash        <- app.runZIO(anyByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentIdsByErgoTreeHash   <- app.runZIO(spentIdsByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentIdsByErgoTreeHash <- app.runZIO(unspentIdsByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyIdsByErgoTreeHash     <- app.runZIO(anyIdsByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
      } yield assertTrue(
        unspentByErgoTreeHash.map(_.boxId).intersect(spentByErgoTreeHash.map(_.boxId)).isEmpty,
        unspentByErgoTreeHash.map(_.boxId).isEmpty,
        spentByErgoTreeHash.map(_.boxId).isEmpty,
        spentByErgoTreeHash.size + unspentByErgoTreeHash.size == anyByErgoTreeHash.size,
        unspentIdsByErgoTreeHash.intersect(spentIdsByErgoTreeHash).isEmpty,
        unspentIdsByErgoTreeHash.isEmpty,
        spentIdsByErgoTreeHash.isEmpty,
        spentIdsByErgoTreeHash.size + unspentIdsByErgoTreeHash.size == anyIdsByErgoTreeHash.size
      )
    }.provide(
      H2Backend.layer,
      PersistentBoxRepo.layer,
      BoxService.layer,
      CoreConf.layer
    ),
    test("get spent/unspent/any box templates by ergo-tree-hash with index filter") {
      val spentByErgoTreeHashT8Get =
        Request.get(URL(Root / "boxes" / "spent" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))
      val unspentByErgoTreeHashT8Get =
        Request.get(URL(Root / "boxes" / "unspent" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))
      val anyByErgoTreeHashT8Get =
        Request.get(URL(Root / "boxes" / "any" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))

      val spentIdsByErgoTreeHashT8Get =
        Request.get(URL(Root / "box-ids" / "spent" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))
      val unspentIdsByErgoTreeHashT8Get =
        Request.get(URL(Root / "box-ids" / "unspent" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))
      val anyIdsByErgoTreeHashT8Get =
        Request.get(URL(Root / "box-ids" / "any" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))

      for {
        spentByErgoTreeT8Hash      <- app.runZIO(spentByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentByErgoTreeT8Hash    <- app.runZIO(unspentByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyByErgoTreeT8Hash        <- app.runZIO(anyByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentIdsByErgoTreeT8Hash   <- app.runZIO(spentIdsByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentIdsByErgoTreeT8Hash <- app.runZIO(unspentIdsByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyIdsByErgoTreeT8Hash     <- app.runZIO(anyIdsByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
      } yield assertTrue(
        unspentByErgoTreeT8Hash.map(_.boxId).intersect(spentByErgoTreeT8Hash.map(_.boxId)).isEmpty,
        unspentByErgoTreeT8Hash.map(_.boxId).isEmpty,
        spentByErgoTreeT8Hash.map(_.boxId).isEmpty,
        spentByErgoTreeT8Hash.size + unspentByErgoTreeT8Hash.size == anyByErgoTreeT8Hash.size,
        unspentIdsByErgoTreeT8Hash.intersect(spentIdsByErgoTreeT8Hash).isEmpty,
        unspentIdsByErgoTreeT8Hash.isEmpty,
        spentIdsByErgoTreeT8Hash.isEmpty,
        spentIdsByErgoTreeT8Hash.size + unspentIdsByErgoTreeT8Hash.size == anyIdsByErgoTreeT8Hash.size
      )
    }.provide(
      H2Backend.layer,
      PersistentBoxRepo.layer,
      BoxService.layer,
      CoreConf.layer
    ),
    test("get assets and boxes by tokenId") {
      val unspentAssetsByTokenIdGet = Request.get(URL(Root / "assets" / "unspent" / "by-token-id" / Protocol.firstBlockRewardBox.unwrapped))
      val spentAssetsByTokenIdGet   = Request.get(URL(Root / "assets" / "spent" / "by-token-id" / Protocol.firstBlockRewardBox.unwrapped))
      val anyAssetsByTokenIdGet     = Request.get(URL(Root / "assets" / "any" / "by-token-id" / Protocol.firstBlockRewardBox.unwrapped))

      val unspentBoxesByTokenIdGet  = Request.get(URL(Root / "boxes" / "unspent" / "by-token-id" / Protocol.firstBlockRewardBox.unwrapped))
      val unspentBoxIdsByTokenIdGet = Request.get(URL(Root / "box-ids" / "unspent" / "by-token-id" / Protocol.firstBlockRewardBox.unwrapped))
      val spentBoxesByTokenIdGet    = Request.get(URL(Root / "boxes" / "spent" / "by-token-id" / Protocol.firstBlockRewardBox.unwrapped))
      val spentBoxIdsByTokenIdGet   = Request.get(URL(Root / "box-ids" / "spent" / "by-token-id" / Protocol.firstBlockRewardBox.unwrapped))
      val anyBoxesByTokenIdGet      = Request.get(URL(Root / "boxes" / "any" / "by-token-id" / Protocol.firstBlockRewardBox.unwrapped))
      val anyBoxIdsByTokenIdGet     = Request.get(URL(Root / "box-ids" / "any" / "by-token-id" / Protocol.firstBlockRewardBox.unwrapped))

      for {
        unspentAssets <- app.runZIO(unspentAssetsByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Asset]])))
        spentAssets   <- app.runZIO(spentAssetsByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Asset]])))
        anyAssets     <- app.runZIO(anyAssetsByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Asset]])))
        unspentBoxes  <- app.runZIO(unspentBoxesByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Asset]])))
        unspentBoxIds <- app.runZIO(unspentBoxIdsByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        spentBoxes    <- app.runZIO(spentBoxesByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Asset]])))
        spentBoxIds   <- app.runZIO(spentBoxIdsByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyBoxes      <- app.runZIO(anyBoxesByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Asset]])))
        anyBoxIds     <- app.runZIO(anyBoxIdsByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
      } yield assertTrue(
        unspentAssets.isEmpty,
        spentAssets.isEmpty,
        anyAssets.isEmpty,
        unspentBoxes.isEmpty,
        unspentBoxIds.isEmpty,
        spentBoxes.isEmpty,
        spentBoxIds.isEmpty,
        anyBoxes.isEmpty,
        anyBoxIds.isEmpty
      )
    }.provide(
      H2Backend.layer,
      PersistentBoxRepo.layer,
      BoxService.layer,
      CoreConf.layer
    )
  ) @@ TestAspect.beforeAll(
    (for
      repo   <- ZIO.service[Repo]
      blocks <- Rest.chain.forHeights(1 to 10)
      _      <- ZIO.collectAllDiscard(blocks.map(b => repo.writeBlock(b)(ZIO.unit, ZIO.unit)))
    yield ()).provide(
      H2Backend.layer,
      PersistentBlockRepo.layer,
      PersistentBoxRepo.layer,
      PersistentRepo.layer
    )
  )
}
