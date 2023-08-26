package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.BlockId.unwrapped
import org.ergoplatform.uexplorer.BoxId.unwrapped
import org.ergoplatform.uexplorer.backend.Codecs
import org.ergoplatform.uexplorer.db.{Asset2Box, Block, Box, Utxo}
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId, TxId}
import sttp.model.QueryParams
import sttp.tapir.generic.auto.*
import sttp.tapir.json.zio.*
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.swagger.bundle.SwaggerInterpreter
import sttp.tapir.ztapir.*
import sttp.tapir.{queryParams, PublicEndpoint, Schema}
import zio.http.{HttpApp, Server}
import zio.json.*
import zio.*

trait BoxesByErgoTree extends Codecs:

  val spentContractBoxesByErgoTree: PublicEndpoint[(String, QueryParams), String, Iterable[Box], Any] =
    endpoint.get
      .in("boxes" / "spent" / "contracts" / "by-ergo-tree" / path[String]("ergoTree"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[Box]])

  val spentContractBoxesByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    spentContractBoxesByErgoTree.zServerLogic { case (ergoTree, qp) =>
      BoxService
        .getSpentBoxesByErgoTree(ergoTree, qp.toMap)
        .mapError(_.getMessage)
    }

  val spentContractBoxIdsByErgoTree: PublicEndpoint[(String, QueryParams), String, Iterable[BoxId], Any] =
    endpoint.get
      .in("box-ids" / "spent" / "contracts" / "by-ergo-tree" / path[String]("ergoTree"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[BoxId]])

  val spentContractBoxIdsByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    spentContractBoxIdsByErgoTree.zServerLogic { case (ergoTree, qp) =>
      BoxService
        .getSpentBoxesByErgoTree(ergoTree, qp.toMap)
        .mapBoth(_.getMessage, _.map(_.boxId))
    }

  val unspentContractBoxesByErgoTree: PublicEndpoint[(String, QueryParams), String, Iterable[Utxo], Any] =
    endpoint.get
      .in("boxes" / "unspent" / "contracts" / "by-ergo-tree" / path[String]("ergoTree"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[Utxo]])

  val unspentContractBoxesByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentContractBoxesByErgoTree.zServerLogic { case (ergoTree, qp) =>
      BoxService
        .getUnspentBoxesByErgoTree(ergoTree, qp.toMap)
        .mapError(_.getMessage)
    }

  val unspentContractBoxIdsByErgoTree: PublicEndpoint[(String, QueryParams), String, Iterable[BoxId], Any] =
    endpoint.get
      .in("box-ids" / "unspent" / "contracts" / "by-ergo-tree" / path[String]("ergoTree"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[BoxId]])

  val unspentContractBoxIdsByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentContractBoxIdsByErgoTree.zServerLogic { case (ergoTree, qp) =>
      BoxService
        .getUnspentBoxesByErgoTree(ergoTree, qp.toMap)
        .mapBoth(_.getMessage, _.map(_.boxId))
    }

  val anyContractBoxesByErgoTree: PublicEndpoint[(String, QueryParams), String, Iterable[Box], Any] =
    endpoint.get
      .in("boxes" / "any" / "contracts" / "by-ergo-tree" / path[String]("ergoTree"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[Box]])

  val anyContractBoxesByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    anyContractBoxesByErgoTree.zServerLogic { case (ergoTree, qp) =>
      BoxService
        .getAnyBoxesByErgoTree(ergoTree, qp.toMap)
        .mapError(_.getMessage)
    }

  val anyContractBoxIdsByErgoTree: PublicEndpoint[(String, QueryParams), String, Iterable[BoxId], Any] =
    endpoint.get
      .in("box-ids" / "any" / "contracts" / "by-ergo-tree" / path[String]("ergoTree"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[BoxId]])

  val anyContractBoxIdsByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    anyContractBoxIdsByErgoTree.zServerLogic { case (ergoTree, qp) =>
      BoxService
        .getAnyBoxesByErgoTree(ergoTree, qp.toMap)
        .mapBoth(_.getMessage, _.map(_.boxId))
    }
