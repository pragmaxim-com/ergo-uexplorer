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
import zio.*
import zio.http.{HttpApp, Server}
import zio.json.*

trait BoxesByErgoTreeHash extends Codecs:

  protected[backend] val spentContractBoxesByErgoTreeHash: PublicEndpoint[(String, QueryParams), String, Iterable[Box], Any] =
    endpoint.get
      .in("boxes" / "spent" / "contracts" / "by-ergo-tree-hash" / path[String]("ergoTreeHash"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[Box]])

  protected[backend] val spentContractBoxesByErgoTreeHashEndpoint: ZServerEndpoint[BoxService, Any] =
    spentContractBoxesByErgoTreeHash.zServerLogic { case (ergoTreeHash, qp) =>
      BoxService
        .getSpentBoxesByErgoTreeHash(ergoTreeHash, qp.toMap)
        .mapError(_.getMessage)
    }

  protected[backend] val spentContractBoxIdsByErgoTreeHash: PublicEndpoint[(String, QueryParams), String, Iterable[BoxId], Any] =
    endpoint.get
      .in("box-ids" / "spent" / "contracts" / "by-ergo-tree-hash" / path[String]("ergoTreeHash"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[BoxId]])

  protected[backend] val spentContractBoxIdsByErgoTreeHashEndpoint: ZServerEndpoint[BoxService, Any] =
    spentContractBoxIdsByErgoTreeHash.zServerLogic { case (ergoTreeHash, qp) =>
      BoxService
        .getSpentBoxesByErgoTreeHash(ergoTreeHash, qp.toMap)
        .mapBoth(_.getMessage, _.map(_.boxId))
    }

  protected[backend] val unspentContractBoxesByErgoTreeHash: PublicEndpoint[(String, QueryParams), String, Iterable[Utxo], Any] =
    endpoint.get
      .in("boxes" / "unspent" / "contracts" / "by-ergo-tree-hash" / path[String]("ergoTreeHash"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[Utxo]])

  protected[backend] val unspentContractBoxesByErgoTreeHashEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentContractBoxesByErgoTreeHash.zServerLogic { case (ergoTreeHash, qp) =>
      BoxService
        .getUnspentBoxesByErgoTreeHash(ergoTreeHash, qp.toMap)
        .mapError(_.getMessage)
    }

  protected[backend] val unspentContractBoxIdsByErgoTreeHash: PublicEndpoint[(String, QueryParams), String, Iterable[BoxId], Any] =
    endpoint.get
      .in("box-ids" / "unspent" / "contracts" / "by-ergo-tree-hash" / path[String]("ergoTreeHash"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[BoxId]])

  protected[backend] val unspentContractBoxIdsByErgoTreeHashEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentContractBoxIdsByErgoTreeHash.zServerLogic { case (ergoTreeHash, qp) =>
      BoxService
        .getUnspentBoxesByErgoTreeHash(ergoTreeHash, qp.toMap)
        .mapBoth(_.getMessage, _.map(_.boxId))
    }

  protected[backend] val anyContractBoxesByErgoTreeHash: PublicEndpoint[(String, QueryParams), String, Iterable[Box], Any] =
    endpoint.get
      .in("boxes" / "any" / "contracts" / "by-ergo-tree-hash" / path[String]("ergoTreeHash"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[Box]])

  protected[backend] val anyContractBoxesByErgoTreeHashEndpoint: ZServerEndpoint[BoxService, Any] =
    anyContractBoxesByErgoTreeHash.zServerLogic { case (ergoTreeHash, qp) =>
      BoxService
        .getAnyBoxesByErgoTreeHash(ergoTreeHash, qp.toMap)
        .mapError(_.getMessage)
    }

  protected[backend] val anyContractBoxIdsByErgoTreeHash: PublicEndpoint[(String, QueryParams), String, Iterable[BoxId], Any] =
    endpoint.get
      .in("box-ids" / "any" / "contracts" / "by-ergo-tree-hash" / path[String]("ergoTreeHash"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[BoxId]])

  protected[backend] val anyContractBoxIdsByErgoTreeHashEndpoint: ZServerEndpoint[BoxService, Any] =
    anyContractBoxIdsByErgoTreeHash.zServerLogic { case (ergoTreeHash, qp) =>
      BoxService
        .getAnyBoxesByErgoTreeHash(ergoTreeHash, qp.toMap)
        .mapBoth(_.getMessage, _.map(_.boxId))
    }
