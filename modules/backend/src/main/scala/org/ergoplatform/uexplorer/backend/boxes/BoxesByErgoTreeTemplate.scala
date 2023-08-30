package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.BlockId.unwrapped
import org.ergoplatform.uexplorer.BoxId.unwrapped
import org.ergoplatform.uexplorer.backend.{Codecs, ErrorResponse}
import org.ergoplatform.uexplorer.db.{Asset2Box, Block, Box, Utxo}
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId, TxId}
import sttp.model.{QueryParams, StatusCode}
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
import org.ergoplatform.uexplorer.backend.IdParsingException

trait BoxesByErgoTreeTemplate extends Codecs:

  protected[backend] val spentTemplateBoxesByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[Box], Any] =
    endpoint.get
      .in("boxes" / "spent" / "templates" / "by-ergo-tree" / path[String]("ergoTreeT8"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[Box]])
      .description("Get spent boxes by ErgoTree template (base16)")

  protected[backend] val spentTemplateBoxesByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    spentTemplateBoxesByErgoTree.zServerLogic { case (ergoTreeT8, qp) =>
      BoxService
        .getSpentBoxesByErgoTreeT8(ergoTreeT8, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val spentTemplateBoxIdsByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxId], Any] =
    endpoint.get
      .in("box-ids" / "spent" / "templates" / "by-ergo-tree" / path[String]("ergoTreeT8"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxId]])
      .description("Get spent box IDs by ErgoTree template (base16)")

  protected[backend] val spentTemplateBoxIdsByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    spentTemplateBoxIdsByErgoTree.zServerLogic { case (templates, qp) =>
      BoxService
        .getSpentBoxesByErgoTreeT8(templates, qp.toMap)
        .map(_.map(_.boxId))
        .mapError(handleThrowable)
    }

  protected[backend] val unspentTemplateBoxesByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[Utxo], Any] =
    endpoint.get
      .in("boxes" / "unspent" / "templates" / "by-ergo-tree" / path[String]("ergoTreeT8"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[Utxo]])
      .description("Get unspent boxes by ErgoTree template (base16)")

  protected[backend] val unspentTemplateBoxesByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentTemplateBoxesByErgoTree.zServerLogic { case (ergoTreeT8, qp) =>
      BoxService
        .getUnspentBoxesByErgoTreeT8(ergoTreeT8, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val unspentTemplateBoxIdsByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxId], Any] =
    endpoint.get
      .in("box-ids" / "unspent" / "templates" / "by-ergo-tree" / path[String]("ergoTreeT8"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxId]])
      .description("Get spent box IDs by ErgoTree template (base16)")

  protected[backend] val unspentTemplateBoxIdsByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentTemplateBoxIdsByErgoTree.zServerLogic { case (ergoTreeT8, qp) =>
      BoxService
        .getUnspentBoxesByErgoTreeT8(ergoTreeT8, qp.toMap)
        .map(_.map(_.boxId))
        .mapError(handleThrowable)
    }

  protected[backend] val anyTemplateBoxesByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[Box], Any] =
    endpoint.get
      .in("boxes" / "any" / "templates" / "by-ergo-tree" / path[String]("ergoTreeT8"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[Box]])
      .description("Get any (spent or unspent) boxes by ErgoTree template (base16)")

  protected[backend] val anyTemplateBoxesByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    anyTemplateBoxesByErgoTree.zServerLogic { case (ergoTreeT8, qp) =>
      BoxService
        .getAnyBoxesByErgoTreeT8(ergoTreeT8, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val anyTemplateBoxIdsByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxId], Any] =
    endpoint.get
      .in("box-ids" / "any" / "templates" / "by-ergo-tree" / path[String]("ergoTreeT8"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxId]])
      .description("Get any (spent or unspent) box IDs by ErgoTree template (base16)")

  protected[backend] val anyTemplateBoxIdsByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    anyTemplateBoxIdsByErgoTree.zServerLogic { case (ergoTreeT8, qp) =>
      BoxService
        .getAnyBoxesByErgoTreeT8(ergoTreeT8, qp.toMap)
        .map(_.map(_.boxId))
        .mapError(handleThrowable)
    }
