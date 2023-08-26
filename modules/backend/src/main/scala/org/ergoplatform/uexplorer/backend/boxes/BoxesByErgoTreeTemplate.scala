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

trait BoxesByErgoTreeTemplate extends Codecs:

  val spentTemplateBoxesByErgoTree: PublicEndpoint[(String, QueryParams), String, Iterable[Box], Any] =
    endpoint.get
      .in("boxes" / "spent" / "templates" / "by-ergo-tree" / path[String]("ergoTreeT8"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[Box]])

  val spentTemplateBoxesByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    spentTemplateBoxesByErgoTree.zServerLogic { case (ergoTreeT8, qp) =>
      BoxService
        .getSpentBoxesByErgoTreeT8(ergoTreeT8, qp.toMap)
        .mapError(_.getMessage)
    }

  val spentTemplateBoxIdsByErgoTree: PublicEndpoint[(String, QueryParams), String, Iterable[BoxId], Any] =
    endpoint.get
      .in("box-ids" / "spent" / "templates" / "by-ergo-tree" / path[String]("ergoTreeT8"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[BoxId]])

  val spentTemplateBoxIdsByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    spentTemplateBoxIdsByErgoTree.zServerLogic { case (templates, qp) =>
      BoxService
        .getSpentBoxesByErgoTreeT8(templates, qp.toMap)
        .mapBoth(_.getMessage, _.map(_.boxId))
    }

  val unspentTemplateBoxesByErgoTree: PublicEndpoint[(String, QueryParams), String, Iterable[Utxo], Any] =
    endpoint.get
      .in("boxes" / "unspent" / "templates" / "by-ergo-tree" / path[String]("ergoTreeT8"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[Utxo]])

  val unspentTemplateBoxesByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentTemplateBoxesByErgoTree.zServerLogic { case (ergoTreeT8, qp) =>
      BoxService
        .getUnspentBoxesByErgoTreeT8(ergoTreeT8, qp.toMap)
        .mapError(_.getMessage)
    }

  val unspentTemplateBoxIdsByErgoTree: PublicEndpoint[(String, QueryParams), String, Iterable[BoxId], Any] =
    endpoint.get
      .in("box-ids" / "unspent" / "templates" / "by-ergo-tree" / path[String]("ergoTreeT8"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[BoxId]])

  val unspentTemplateBoxIdsByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentTemplateBoxIdsByErgoTree.zServerLogic { case (ergoTreeT8, qp) =>
      BoxService
        .getUnspentBoxesByErgoTreeT8(ergoTreeT8, qp.toMap)
        .mapBoth(_.getMessage, _.map(_.boxId))
    }

  val anyTemplateBoxesByErgoTree: PublicEndpoint[(String, QueryParams), String, Iterable[Box], Any] =
    endpoint.get
      .in("boxes" / "any" / "templates" / "by-ergo-tree" / path[String]("ergoTreeT8"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[Box]])

  val anyTemplateBoxesByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    anyTemplateBoxesByErgoTree.zServerLogic { case (ergoTreeT8, qp) =>
      BoxService
        .getAnyBoxesByErgoTreeT8(ergoTreeT8, qp.toMap)
        .mapError(_.getMessage)
    }

  val anyTemplateBoxIdsByErgoTree: PublicEndpoint[(String, QueryParams), String, Iterable[BoxId], Any] =
    endpoint.get
      .in("box-ids" / "any" / "templates" / "by-ergo-tree" / path[String]("ergoTreeT8"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[BoxId]])

  val anyTemplateBoxIdsByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    anyTemplateBoxIdsByErgoTree.zServerLogic { case (ergoTreeT8, qp) =>
      BoxService
        .getAnyBoxesByErgoTreeT8(ergoTreeT8, qp.toMap)
        .mapBoth(_.getMessage, _.map(_.boxId))
    }
