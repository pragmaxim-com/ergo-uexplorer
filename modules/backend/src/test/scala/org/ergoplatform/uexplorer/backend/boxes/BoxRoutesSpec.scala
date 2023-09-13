package org.ergoplatform.uexplorer.backend.boxes

import eu.timepit.refined.auto.autoUnwrap
import org.ergoplatform.uexplorer.Const.Protocol
import org.ergoplatform.uexplorer.Const.Protocol.Emission
import org.ergoplatform.uexplorer.backend.blocks.{BlockService, PersistentBlockRepo}
import org.ergoplatform.uexplorer.backend.ZioRoutes
import org.ergoplatform.uexplorer.db.BoxWithAssets
import org.ergoplatform.uexplorer.http.NodePool
import org.ergoplatform.uexplorer.{BoxId, CoreConf}
import zio.*
import zio.http.*
import zio.json.*
import zio.test.*

import scala.collection.immutable.Set

trait BoxRoutesSpec extends ZIOSpec[TestEnvironment] with ZioRoutes {

  private val indexFilter: Map[String, Chunk[String]] = BoxService.indexWhiteList.map(key => key -> Chunk("")).toMap

  def boxRoutesSpec(routes: App[Client with NodePool with BoxService with BlockService]) =
    suite("BoxRoutesSpec")(
      test("get spent/unspent/any box(es) by id") {
        val unspentBoxGet        = Request.get(URL(Root / rootPath / "boxes" / "unspent" / Protocol.firstBlockRewardBox.unwrapped))
        val missingUnspentBoxGet = Request.get(URL(Root / rootPath / "boxes" / "unspent" / Emission.outputBox.unwrapped))
        val spentBoxGet          = Request.get(URL(Root / rootPath / "boxes" / "spent" / Emission.outputBox.unwrapped))
        val missingSpentBoxGet   = Request.get(URL(Root / rootPath / "boxes" / "spent" / Protocol.firstBlockRewardBox.unwrapped))
        val anyBoxGet            = Request.get(URL(Root / rootPath / "boxes" / "any" / Emission.outputBox.unwrapped))

        val unspentBoxPost =
          Request.post(
            Body.fromString(Set(Protocol.firstBlockRewardBox, Emission.outputBox).toJson),
            URL(Root / rootPath / "boxes" / "unspent")
          )
        val spentBoxPost =
          Request.post(
            Body.fromString(Set(Emission.outputBox, Protocol.firstBlockRewardBox).toJson),
            URL(Root / rootPath / "boxes" / "spent")
          )
        val anyBoxPost =
          Request.post(
            Body.fromString(Set(Emission.outputBox, Protocol.firstBlockRewardBox).toJson),
            URL(Root / rootPath / "boxes" / "any")
          )

        for {
          unspentBox              <- routes.runZIO(unspentBoxGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[BoxWithAssets])))
          unspentBoxes            <- routes.runZIO(unspentBoxPost).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Set[BoxWithAssets]])))
          spentBox                <- routes.runZIO(spentBoxGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[BoxWithAssets])))
          anyBox                  <- routes.runZIO(anyBoxGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[BoxWithAssets])))
          spentBoxes              <- routes.runZIO(spentBoxPost).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Set[BoxWithAssets]])))
          anyBoxes                <- routes.runZIO(anyBoxPost).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[Set[BoxWithAssets]])))
          missingSpentBoxStatus   <- routes.runZIO(missingSpentBoxGet).map(_.status)
          missingUnspentBoxStatus <- routes.runZIO(missingUnspentBoxGet).map(_.status)
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
      },
      test("get spent/unspent/any box(es) by address") {
        val spentBoxesByAddressGet   = Request.get(URL(Root / rootPath / "boxes" / "spent" / "by-address" / Emission.address))
        val unspentBoxesByAddressGet = Request.get(URL(Root / rootPath / "boxes" / "unspent" / "by-address" / Emission.address))
        val anyBoxesByAddressGet     = Request.get(URL(Root / rootPath / "boxes" / "any" / "by-address" / Emission.address))

        val spentBoxIdsByAddressGet   = Request.get(URL(Root / rootPath / "box-ids" / "spent" / "by-address" / Emission.address))
        val unspentBoxIdsByAddressGet = Request.get(URL(Root / rootPath / "box-ids" / "unspent" / "by-address" / Emission.address))
        val anyBoxIdsByAddressGet     = Request.get(URL(Root / rootPath / "box-ids" / "any" / "by-address" / Emission.address))

        for {
          spentBoxesByAddress <-
            routes.runZIO(spentBoxesByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          unspentBoxesByAddress <-
            routes.runZIO(unspentBoxesByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          anyBoxesByAddress <- routes.runZIO(anyBoxesByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          spentBoxIdsByAddress <-
            routes.runZIO(spentBoxIdsByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          unspentBoxIdsByAddress <-
            routes.runZIO(unspentBoxIdsByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          anyBoxIdsByAddress <-
            routes.runZIO(anyBoxIdsByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      },
      test("get spent/unspent/any box contracts by ergo-tree") {
        val spentBoxesByErgoTreeGet   = Request.get(URL(Root / rootPath / "boxes" / "spent" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex))
        val unspentBoxesByErgoTreeGet = Request.get(URL(Root / rootPath / "boxes" / "unspent" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex))
        val anyBoxesByErgoTreeGet     = Request.get(URL(Root / rootPath / "boxes" / "any" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex))

        val spentBoxIdsByErgoTreeGet   = Request.get(URL(Root / rootPath / "box-ids" / "spent" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex))
        val unspentBoxIdsByErgoTreeGet = Request.get(URL(Root / rootPath / "box-ids" / "unspent" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex))
        val anyBoxIdsByErgoTreeGet     = Request.get(URL(Root / rootPath / "box-ids" / "any" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex))

        for {
          spentBoxesByErgoTree <-
            routes.runZIO(spentBoxesByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          unspentBoxesByErgoTree <-
            routes.runZIO(unspentBoxesByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          anyBoxesByErgoTree <- routes.runZIO(anyBoxesByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          spentBoxIdsByErgoTree <-
            routes.runZIO(spentBoxIdsByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          unspentBoxIdsByErgoTree <-
            routes.runZIO(unspentBoxIdsByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          anyBoxIdsByErgoTree <-
            routes.runZIO(anyBoxIdsByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      },
      test("get spent/unspent/any box templates by ergo-tree") {
        val spentBoxesByErgoTreeT8Get   = Request.get(URL(Root / rootPath / "boxes" / "spent" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex))
        val unspentBoxesByErgoTreeT8Get = Request.get(URL(Root / rootPath / "boxes" / "unspent" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex))
        val anyBoxesByErgoTreeT8Get     = Request.get(URL(Root / rootPath / "boxes" / "any" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex))

        val spentBoxIdsByErgoTreeT8Get   = Request.get(URL(Root / rootPath / "box-ids" / "spent" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex))
        val unspentBoxIdsByErgoTreeT8Get = Request.get(URL(Root / rootPath / "box-ids" / "unspent" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex))
        val anyBoxIdsByErgoTreeT8Get     = Request.get(URL(Root / rootPath / "box-ids" / "any" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex))
        for {
          spentBoxesByErgoTreeT8 <-
            routes.runZIO(spentBoxesByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          unspentBoxesByErgoTreeT8 <-
            routes.runZIO(unspentBoxesByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          anyBoxesByErgoTreeT8 <-
            routes.runZIO(anyBoxesByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          spentBoxIdsByErgoTreeT8 <-
            routes.runZIO(spentBoxIdsByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          unspentBoxIdsByErgoTreeT8 <-
            routes.runZIO(unspentBoxIdsByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          anyBoxIdsByErgoTreeT8 <-
            routes.runZIO(anyBoxIdsByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      },
      test("get spent/unspent/any box contracts by ergo-tree-hash") {
        val spentBoxesByErgoTreeHashGet   = Request.get(URL(Root / rootPath / "boxes" / "spent" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash))
        val unspentBoxesByErgoTreeHashGet = Request.get(URL(Root / rootPath / "boxes" / "unspent" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash))
        val anyBoxesByErgoTreeHashGet     = Request.get(URL(Root / rootPath / "boxes" / "any" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash))

        val spentBoxIdsByErgoTreeHashGet = Request.get(URL(Root / rootPath / "box-ids" / "spent" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash))
        val unspentBoxIdsByErgoTreeHashGet =
          Request.get(URL(Root / rootPath / "box-ids" / "unspent" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash))
        val anyBoxIdsByErgoTreeHashGet = Request.get(URL(Root / rootPath / "box-ids" / "any" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash))

        for {
          spentBoxesByErgoTreeHash <-
            routes.runZIO(spentBoxesByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          unspentBoxesByErgoTreeHash <-
            routes.runZIO(unspentBoxesByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          anyBoxesByErgoTreeHash <-
            routes.runZIO(anyBoxesByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          spentBoxIdsByErgoTreeHash <-
            routes.runZIO(spentBoxIdsByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          unspentBoxIdsByErgoTreeHash <-
            routes.runZIO(unspentBoxIdsByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          anyBoxIdsByErgoTreeHash <-
            routes.runZIO(anyBoxIdsByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        } yield assertTrue(
          unspentBoxesByErgoTreeHash.map(_.boxId).intersect(spentBoxesByErgoTreeHash.map(_.boxId)).isEmpty,
          unspentBoxesByErgoTreeHash.map(_.boxId).nonEmpty,
          spentBoxesByErgoTreeHash.map(_.boxId).nonEmpty,
          spentBoxesByErgoTreeHash.size + unspentBoxesByErgoTreeHash.size == anyBoxesByErgoTreeHash.size,
          unspentBoxIdsByErgoTreeHash.intersect(spentBoxIdsByErgoTreeHash).isEmpty,
          unspentBoxIdsByErgoTreeHash.nonEmpty,
          spentBoxIdsByErgoTreeHash.nonEmpty,
          spentBoxIdsByErgoTreeHash.size + unspentBoxIdsByErgoTreeHash.size == anyBoxIdsByErgoTreeHash.size
        )
      },
      test("get spent/unspent/any box templates by ergo-tree-hash") {
        val spentByErgoTreeHashT8Get   = Request.get(URL(Root / rootPath / "boxes" / "spent" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash))
        val unspentByErgoTreeHashT8Get = Request.get(URL(Root / rootPath / "boxes" / "unspent" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash))
        val anyByErgoTreeHashT8Get     = Request.get(URL(Root / rootPath / "boxes" / "any" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash))

        val spentIdsByErgoTreeHashT8Get = Request.get(URL(Root / rootPath / "box-ids" / "spent" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash))
        val unspentIdsByErgoTreeHashT8Get =
          Request.get(URL(Root / rootPath / "box-ids" / "unspent" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash))
        val anyIdsByErgoTreeHashT8Get = Request.get(URL(Root / rootPath / "box-ids" / "any" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash))

        for {
          spentByErgoTreeT8Hash <-
            routes.runZIO(spentByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          unspentByErgoTreeT8Hash <-
            routes.runZIO(unspentByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          anyByErgoTreeT8Hash <-
            routes.runZIO(anyByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          spentIdsByErgoTreeT8Hash <-
            routes.runZIO(spentIdsByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          unspentIdsByErgoTreeT8Hash <-
            routes.runZIO(unspentIdsByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          anyIdsByErgoTreeT8Hash <-
            routes.runZIO(anyIdsByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      },
      test("get spent/unspent/any box(es) by address with index filter") {
        val spentByAddressGet   = Request.get(URL(Root / rootPath / "boxes" / "spent" / "by-address" / Emission.address).withQueryParams(indexFilter))
        val unspentByAddressGet = Request.get(URL(Root / rootPath / "boxes" / "unspent" / "by-address" / Emission.address).withQueryParams(indexFilter))
        val anyByAddressGet     = Request.get(URL(Root / rootPath / "boxes" / "any" / "by-address" / Emission.address).withQueryParams(indexFilter))

        val spentIdsByAddressGet   = Request.get(URL(Root / rootPath / "box-ids" / "spent" / "by-address" / Emission.address).withQueryParams(indexFilter))
        val unspentIdsByAddressGet = Request.get(URL(Root / rootPath / "box-ids" / "unspent" / "by-address" / Emission.address).withQueryParams(indexFilter))
        val anyIdsByAddressGet     = Request.get(URL(Root / rootPath / "box-ids" / "any" / "by-address" / Emission.address).withQueryParams(indexFilter))

        for {
          spentByAddress    <- routes.runZIO(spentByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          unspentByAddress  <- routes.runZIO(unspentByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          anyByAddress      <- routes.runZIO(anyByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          spentIdsByAddress <- routes.runZIO(spentIdsByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          unspentIdsByAddress <-
            routes.runZIO(unspentIdsByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          anyIdsByAddress <- routes.runZIO(anyIdsByAddressGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      },
      test("get spent/unspent/any box contracts by ergo-tree with index filter") {
        val spentByErgoTreeGet =
          Request.get(URL(Root / rootPath / "boxes" / "spent" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))
        val unspentByErgoTreeGet =
          Request.get(URL(Root / rootPath / "boxes" / "unspent" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))
        val anyByErgoTreeGet =
          Request.get(URL(Root / rootPath / "boxes" / "any" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))

        val spentIdsByErgoTreeGet =
          Request.get(URL(Root / rootPath / "box-ids" / "spent" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))
        val unspentIdsByErgoTreeGet =
          Request.get(URL(Root / rootPath / "box-ids" / "unspent" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))
        val anyIdsByErgoTreeGet =
          Request.get(URL(Root / rootPath / "boxes" / "any" / "contracts" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))

        for {
          spentByErgoTree   <- routes.runZIO(spentByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          unspentByErgoTree <- routes.runZIO(unspentByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          anyByErgoTree     <- routes.runZIO(anyByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          spentIdsByErgoTree <-
            routes.runZIO(spentIdsByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          unspentIdsByErgoTree <-
            routes.runZIO(unspentIdsByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          anyIdsByErgoTree <- routes.runZIO(anyIdsByErgoTreeGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      },
      test("get spent/unspent/any box templates by ergo-tree with index filter") {

        val spentByErgoTreeT8Get =
          Request.get(URL(Root / rootPath / "boxes" / "spent" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))
        val unspentByErgoTreeT8Get =
          Request.get(URL(Root / rootPath / "boxes" / "unspent" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))
        val anyByErgoTreeT8Get =
          Request.get(URL(Root / rootPath / "boxes" / "any" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))

        val spentIdsByErgoTreeT8Get =
          Request.get(URL(Root / rootPath / "box-ids" / "spent" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))
        val unspentIdsByErgoTreeT8Get =
          Request.get(URL(Root / rootPath / "box-ids" / "unspent" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))
        val anyIdsByErgoTreeT8Get =
          Request.get(URL(Root / rootPath / "box-ids" / "any" / "templates" / "by-ergo-tree" / Emission.ergoTreeHex).withQueryParams(indexFilter))

        for {
          spentByErgoTreeT8 <- routes.runZIO(spentByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          unspentByErgoTreeT8 <-
            routes.runZIO(unspentByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          anyByErgoTreeT8 <- routes.runZIO(anyByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          spentIdsByErgoTreeT8 <-
            routes.runZIO(spentIdsByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          unspentIdsByErgoTreeT8 <-
            routes.runZIO(unspentIdsByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          anyIdsByErgoTreeT8 <-
            routes.runZIO(anyIdsByErgoTreeT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      },
      test("get spent/unspent/any box contracts by ergo-tree-hash with index filter") {
        val spentByErgoTreeHashGet =
          Request.get(URL(Root / rootPath / "boxes" / "spent" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))
        val unspentByErgoTreeHashGet =
          Request.get(URL(Root / rootPath / "boxes" / "unspent" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))
        val anyByErgoTreeHashGet =
          Request.get(URL(Root / rootPath / "boxes" / "any" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))

        val spentIdsByErgoTreeHashGet =
          Request.get(URL(Root / rootPath / "box-ids" / "spent" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))
        val unspentIdsByErgoTreeHashGet =
          Request.get(URL(Root / rootPath / "box-ids" / "unspent" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))
        val anyIdsByErgoTreeHashGet =
          Request.get(URL(Root / rootPath / "box-ids" / "any" / "contracts" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))

        for {
          spentByErgoTreeHash <-
            routes.runZIO(spentByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          unspentByErgoTreeHash <-
            routes.runZIO(unspentByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          anyByErgoTreeHash <- routes.runZIO(anyByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          spentIdsByErgoTreeHash <-
            routes.runZIO(spentIdsByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          unspentIdsByErgoTreeHash <-
            routes.runZIO(unspentIdsByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          anyIdsByErgoTreeHash <-
            routes.runZIO(anyIdsByErgoTreeHashGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      },
      test("get spent/unspent/any box templates by ergo-tree-hash with index filter") {
        val spentByErgoTreeHashT8Get =
          Request.get(URL(Root / rootPath / "boxes" / "spent" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))
        val unspentByErgoTreeHashT8Get =
          Request.get(URL(Root / rootPath / "boxes" / "unspent" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))
        val anyByErgoTreeHashT8Get =
          Request.get(URL(Root / rootPath / "boxes" / "any" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))

        val spentIdsByErgoTreeHashT8Get =
          Request.get(URL(Root / rootPath / "box-ids" / "spent" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))
        val unspentIdsByErgoTreeHashT8Get =
          Request.get(URL(Root / rootPath / "box-ids" / "unspent" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))
        val anyIdsByErgoTreeHashT8Get =
          Request.get(URL(Root / rootPath / "box-ids" / "any" / "templates" / "by-ergo-tree-hash" / Emission.ergoTreeHash).withQueryParams(indexFilter))

        for {
          spentByErgoTreeT8Hash <-
            routes.runZIO(spentByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          unspentByErgoTreeT8Hash <-
            routes.runZIO(unspentByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          anyByErgoTreeT8Hash <-
            routes.runZIO(anyByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          spentIdsByErgoTreeT8Hash <-
            routes.runZIO(spentIdsByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          unspentIdsByErgoTreeT8Hash <-
            routes.runZIO(unspentIdsByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          anyIdsByErgoTreeT8Hash <-
            routes.runZIO(anyIdsByErgoTreeHashT8Get).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
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
      },
      test("get boxes by tokenId") {
        val unspentBoxesByTokenIdGet  = Request.get(URL(Root / rootPath / "boxes" / "unspent" / "by-token-id" / Protocol.firstBlockRewardBox.unwrapped))
        val unspentBoxIdsByTokenIdGet = Request.get(URL(Root / rootPath / "box-ids" / "unspent" / "by-token-id" / Protocol.firstBlockRewardBox.unwrapped))
        val spentBoxesByTokenIdGet    = Request.get(URL(Root / rootPath / "boxes" / "spent" / "by-token-id" / Protocol.firstBlockRewardBox.unwrapped))
        val spentBoxIdsByTokenIdGet   = Request.get(URL(Root / rootPath / "box-ids" / "spent" / "by-token-id" / Protocol.firstBlockRewardBox.unwrapped))
        val anyBoxesByTokenIdGet      = Request.get(URL(Root / rootPath / "boxes" / "any" / "by-token-id" / Protocol.firstBlockRewardBox.unwrapped))
        val anyBoxIdsByTokenIdGet     = Request.get(URL(Root / rootPath / "box-ids" / "any" / "by-token-id" / Protocol.firstBlockRewardBox.unwrapped))

        for {
          unspentBoxes  <- routes.runZIO(unspentBoxesByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          unspentBoxIds <- routes.runZIO(unspentBoxIdsByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          spentBoxes    <- routes.runZIO(spentBoxesByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          spentBoxIds   <- routes.runZIO(spentBoxIdsByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
          anyBoxes      <- routes.runZIO(anyBoxesByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxWithAssets]])))
          anyBoxIds     <- routes.runZIO(anyBoxIdsByTokenIdGet).flatMap(_.body.asString.flatMap(x => ZIO.fromEither(x.fromJson[List[BoxId]])))
        } yield assertTrue(
          unspentBoxes.isEmpty,
          unspentBoxIds.isEmpty,
          spentBoxes.isEmpty,
          spentBoxIds.isEmpty,
          anyBoxes.isEmpty,
          anyBoxIds.isEmpty
        )
      }
    ).provide(routeLayers)
}
