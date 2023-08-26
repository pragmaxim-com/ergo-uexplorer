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

trait BoxesByErgoTreeTemplateHash extends Codecs:

  val spentTemplateBoxesByErgoTreeHash: PublicEndpoint[(String, QueryParams), String, Iterable[Box], Any] =
    endpoint.get
      .in("boxes" / "spent" / "templates" / "by-ergo-tree-hash" / path[String]("ergoTreeT8Hash"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[Box]])

  val spentTemplateBoxesByErgoTreeHashEndpoint: ZServerEndpoint[BoxService, Any] =
    spentTemplateBoxesByErgoTreeHash.zServerLogic { case (ergoTreeT8Hash, qp) =>
      BoxService
        .getSpentBoxesByErgoTreeT8Hash(ergoTreeT8Hash, qp.toMap)
        .mapError(_.getMessage)
    }

  val spentTemplateBoxIdsByErgoTreeHash: PublicEndpoint[(String, QueryParams), String, Iterable[BoxId], Any] =
    endpoint.get
      .in("box-ids" / "spent" / "templates" / "by-ergo-tree-hash" / path[String]("ergoTreeT8Hash"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[BoxId]])

  val spentTemplateBoxIdsByErgoTreeHashEndpoint: ZServerEndpoint[BoxService, Any] =
    spentTemplateBoxIdsByErgoTreeHash.zServerLogic { case (ergoTreeT8Hash, qp) =>
      BoxService
        .getSpentBoxesByErgoTreeT8Hash(ergoTreeT8Hash, qp.toMap)
        .mapBoth(_.getMessage, _.map(_.boxId))
    }

  val unspentTemplateBoxesByErgoTreeHash: PublicEndpoint[(String, QueryParams), String, Iterable[Utxo], Any] =
    endpoint.get
      .in("boxes" / "unspent" / "templates" / "by-ergo-tree-hash" / path[String]("ergoTreeT8Hash"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[Utxo]])

  val unspentTemplateBoxesByErgoTreeHashEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentTemplateBoxesByErgoTreeHash.zServerLogic { case (ergoTreeT8Hash, qp) =>
      BoxService
        .getUnspentBoxesByErgoTreeT8Hash(ergoTreeT8Hash, qp.toMap)
        .mapError(_.getMessage)
    }

  val unspentTemplateBoxIdsByErgoTreeHash: PublicEndpoint[(String, QueryParams), String, Iterable[BoxId], Any] =
    endpoint.get
      .in("box-ids" / "unspent" / "templates" / "by-ergo-tree-hash" / path[String]("ergoTreeT8Hash"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[BoxId]])

  val unspentTemplateBoxIdsByErgoTreeHashEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentTemplateBoxIdsByErgoTreeHash.zServerLogic { case (ergoTreeT8Hash, qp) =>
      BoxService
        .getUnspentBoxesByErgoTreeT8Hash(ergoTreeT8Hash, qp.toMap)
        .mapBoth(_.getMessage, _.map(_.boxId))
    }

  val anyTemplateBoxesByErgoTreeHash: PublicEndpoint[(String, QueryParams), String, Iterable[Box], Any] =
    endpoint.get
      .in("boxes" / "any" / "templates" / "by-ergo-tree-hash" / path[String]("ergoTreeT8Hash"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[Box]])

  val anyTemplateBoxesByErgoTreeHashEndpoint: ZServerEndpoint[BoxService, Any] =
    anyTemplateBoxesByErgoTreeHash.zServerLogic { case (ergoTreeT8Hash, qp) =>
      BoxService
        .getAnyBoxesByErgoTreeT8Hash(ergoTreeT8Hash, qp.toMap)
        .mapError(_.getMessage)
    }

  val anyTemplateBoxIdsByErgoTreeHash: PublicEndpoint[(String, QueryParams), String, Iterable[BoxId], Any] =
    endpoint.get
      .in("box-ids" / "any" / "templates" / "by-ergo-tree-hash" / path[String]("ergoTreeT8Hash"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[BoxId]])

  val anyTemplateBoxIdsByErgoTreeHashEndpoint: ZServerEndpoint[BoxService, Any] =
    anyTemplateBoxIdsByErgoTreeHash.zServerLogic { case (ergoTreeT8Hash, qp) =>
      BoxService
        .getAnyBoxesByErgoTreeT8Hash(ergoTreeT8Hash, qp.toMap)
        .mapBoth(_.getMessage, _.map(_.boxId))
    }
