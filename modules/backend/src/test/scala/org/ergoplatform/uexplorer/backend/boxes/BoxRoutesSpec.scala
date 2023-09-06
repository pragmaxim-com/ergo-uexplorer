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

trait BoxRoutesSpec extends ZIOSpec[TestEnvironment] {

  private val boxRoutes                               = BoxRoutes()
  private val indexFilter: Map[String, Chunk[String]] = BoxService.indexWhiteList.map(key => key -> Chunk("")).toMap

  def boxRoutesSpec = suite("BoxRoutesSpec")(
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
        unspentBox              <- boxRoutes.runZIO(unspentBoxGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Utxo])))
        unspentBoxes            <- boxRoutes.runZIO(unspentBoxPost).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Set[Utxo]])))
        spentBox                <- boxRoutes.runZIO(spentBoxGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Box])))
        anyBox                  <- boxRoutes.runZIO(anyBoxGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Box])))
        spentBoxes              <- boxRoutes.runZIO(spentBoxPost).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Set[Box]])))
        anyBoxes                <- boxRoutes.runZIO(anyBoxPost).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Set[Box]])))
        missingSpentBoxStatus   <- boxRoutes.runZIO(missingSpentBoxGet).map(_.status)
        missingUnspentBoxStatus <- boxRoutes.runZIO(missingUnspentBoxGet).map(_.status)
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
      H2Backend.zLayerFromConf,
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
        spentBoxesByAddress    <- boxRoutes.runZIO(spentBoxesByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentBoxesByAddress  <- boxRoutes.runZIO(unspentBoxesByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyBoxesByAddress      <- boxRoutes.runZIO(anyBoxesByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentBoxIdsByAddress   <- boxRoutes.runZIO(spentBoxIdsByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentBoxIdsByAddress <- boxRoutes.runZIO(unspentBoxIdsByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyBoxIdsByAddress     <- boxRoutes.runZIO(anyBoxIdsByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      H2Backend.zLayerFromConf,
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
        spentBoxesByErgoTree    <- boxRoutes.runZIO(spentBoxesByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentBoxesByErgoTree  <- boxRoutes.runZIO(unspentBoxesByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyBoxesByErgoTree      <- boxRoutes.runZIO(anyBoxesByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentBoxIdsByErgoTree   <- boxRoutes.runZIO(spentBoxIdsByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentBoxIdsByErgoTree <- boxRoutes.runZIO(unspentBoxIdsByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyBoxIdsByErgoTree     <- boxRoutes.runZIO(anyBoxIdsByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      H2Backend.zLayerFromConf,
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
        spentBoxesByErgoTreeT8   <- boxRoutes.runZIO(spentBoxesByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentBoxesByErgoTreeT8 <- boxRoutes.runZIO(unspentBoxesByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyBoxesByErgoTreeT8     <- boxRoutes.runZIO(anyBoxesByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentBoxIdsByErgoTreeT8  <- boxRoutes.runZIO(spentBoxIdsByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentBoxIdsByErgoTreeT8 <-
          boxRoutes.runZIO(unspentBoxIdsByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyBoxIdsByErgoTreeT8 <- boxRoutes.runZIO(anyBoxIdsByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      H2Backend.zLayerFromConf,
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
        spentBoxesByErgoTreeHash <- boxRoutes.runZIO(spentBoxesByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentBoxesByErgoTreeHash <-
          boxRoutes.runZIO(unspentBoxesByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyBoxesByErgoTreeHash <- boxRoutes.runZIO(anyBoxesByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentBoxIdsByErgoTreeHash <-
          boxRoutes.runZIO(spentBoxIdsByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentBoxIdsByErgoTreeHash <-
          boxRoutes.runZIO(unspentBoxIdsByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyBoxIdsByErgoTreeHash <- boxRoutes.runZIO(anyBoxIdsByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      H2Backend.zLayerFromConf,
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
        spentByErgoTreeT8Hash    <- boxRoutes.runZIO(spentByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentByErgoTreeT8Hash  <- boxRoutes.runZIO(unspentByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyByErgoTreeT8Hash      <- boxRoutes.runZIO(anyByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentIdsByErgoTreeT8Hash <- boxRoutes.runZIO(spentIdsByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentIdsByErgoTreeT8Hash <-
          boxRoutes.runZIO(unspentIdsByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyIdsByErgoTreeT8Hash <- boxRoutes.runZIO(anyIdsByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      H2Backend.zLayerFromConf,
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
        spentByAddress      <- boxRoutes.runZIO(spentByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentByAddress    <- boxRoutes.runZIO(unspentByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyByAddress        <- boxRoutes.runZIO(anyByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentIdsByAddress   <- boxRoutes.runZIO(spentIdsByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentIdsByAddress <- boxRoutes.runZIO(unspentIdsByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyIdsByAddress     <- boxRoutes.runZIO(anyIdsByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      H2Backend.zLayerFromConf,
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
        spentByErgoTree      <- boxRoutes.runZIO(spentByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentByErgoTree    <- boxRoutes.runZIO(unspentByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyByErgoTree        <- boxRoutes.runZIO(anyByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentIdsByErgoTree   <- boxRoutes.runZIO(spentIdsByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentIdsByErgoTree <- boxRoutes.runZIO(unspentIdsByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyIdsByErgoTree     <- boxRoutes.runZIO(anyIdsByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      H2Backend.zLayerFromConf,
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
        spentByErgoTreeT8      <- boxRoutes.runZIO(spentByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentByErgoTreeT8    <- boxRoutes.runZIO(unspentByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyByErgoTreeT8        <- boxRoutes.runZIO(anyByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentIdsByErgoTreeT8   <- boxRoutes.runZIO(spentIdsByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentIdsByErgoTreeT8 <- boxRoutes.runZIO(unspentIdsByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyIdsByErgoTreeT8     <- boxRoutes.runZIO(anyIdsByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      H2Backend.zLayerFromConf,
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
        spentByErgoTreeHash      <- boxRoutes.runZIO(spentByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentByErgoTreeHash    <- boxRoutes.runZIO(unspentByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyByErgoTreeHash        <- boxRoutes.runZIO(anyByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentIdsByErgoTreeHash   <- boxRoutes.runZIO(spentIdsByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentIdsByErgoTreeHash <- boxRoutes.runZIO(unspentIdsByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyIdsByErgoTreeHash     <- boxRoutes.runZIO(anyIdsByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      H2Backend.zLayerFromConf,
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
        spentByErgoTreeT8Hash    <- boxRoutes.runZIO(spentByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        unspentByErgoTreeT8Hash  <- boxRoutes.runZIO(unspentByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Utxo]])))
        anyByErgoTreeT8Hash      <- boxRoutes.runZIO(anyByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Box]])))
        spentIdsByErgoTreeT8Hash <- boxRoutes.runZIO(spentIdsByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        unspentIdsByErgoTreeT8Hash <-
          boxRoutes.runZIO(unspentIdsByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyIdsByErgoTreeT8Hash <- boxRoutes.runZIO(anyIdsByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      H2Backend.zLayerFromConf,
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
        unspentAssets <- boxRoutes.runZIO(unspentAssetsByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Asset]])))
        spentAssets   <- boxRoutes.runZIO(spentAssetsByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Asset]])))
        anyAssets     <- boxRoutes.runZIO(anyAssetsByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Asset]])))
        unspentBoxes  <- boxRoutes.runZIO(unspentBoxesByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Asset]])))
        unspentBoxIds <- boxRoutes.runZIO(unspentBoxIdsByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        spentBoxes    <- boxRoutes.runZIO(spentBoxesByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Asset]])))
        spentBoxIds   <- boxRoutes.runZIO(spentBoxIdsByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        anyBoxes      <- boxRoutes.runZIO(anyBoxesByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[Asset]])))
        anyBoxIds     <- boxRoutes.runZIO(anyBoxIdsByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      H2Backend.zLayerFromConf,
      PersistentBoxRepo.layer,
      BoxService.layer,
      CoreConf.layer
    )
  )
}
