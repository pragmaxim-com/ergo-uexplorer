package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.backend.{Codecs, ErrorResponse, TapirRoutes}
import org.ergoplatform.uexplorer.db.{Box, BoxWithAssets, Utxo}
import org.ergoplatform.uexplorer.{BlockId, BoxId, TxId}
import sttp.model.{QueryParams, StatusCode}
import sttp.tapir.generic.auto.*
import sttp.tapir.json.zio.*
import sttp.tapir.ztapir.*
import sttp.tapir.{queryParams, PublicEndpoint}
import zio.*
import zio.json.*

trait BoxesByIdRoutes extends TapirRoutes with Codecs:

  protected[backend] val unspentBoxById: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), BoxWithAssets, Any] =
    endpoint.get
      .in(rootPath / "boxes" / "unspent" / path[String]("boxId"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[BoxWithAssets])
      .description("Get unspent box by box ID")

  protected[backend] def unspentBoxByIdEndpoint(implicit enc: ErgoAddressEncoder): ZServerEndpoint[BoxService, Any] =
    unspentBoxById.zServerLogic { case (boxId, qp) =>
      BoxService
        .getUtxo(boxId, qp.toMap)
        .mapError(handleThrowable)
        .flatMap {
          case None =>
            ZIO.fail(ErrorResponse(StatusCode.NotFound.code, "not-found") -> StatusCode.NotFound)
          case Some(utxo) =>
            ZIO.succeed(utxo)
        }
    }

  protected[backend] val unspentBoxesByIds: PublicEndpoint[(Set[String], QueryParams), (ErrorResponse, StatusCode), List[BoxWithAssets], Any] =
    endpoint.post
      .in(rootPath / "boxes" / "unspent")
      .in(jsonBody[Set[String]])
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[List[BoxWithAssets]])

  protected[backend] def unspentBoxesByIdEndpoint(implicit enc: ErgoAddressEncoder): ZServerEndpoint[BoxService, Any] =
    unspentBoxesByIds.zServerLogic { case (boxIds, qp) =>
      BoxService
        .getUtxos(boxIds, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val spentBoxById: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), BoxWithAssets, Any] =
    endpoint.get
      .in(rootPath / "boxes" / "spent" / path[String]("boxId"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[BoxWithAssets])

  protected[backend] def spentBoxByIdEndpoint(implicit enc: ErgoAddressEncoder): ZServerEndpoint[BoxService, Any] =
    spentBoxById.zServerLogic { case (boxId, qp) =>
      BoxService
        .getSpentBox(boxId, qp.toMap)
        .mapError(handleThrowable)
        .flatMap {
          case None =>
            ZIO.fail(ErrorResponse(StatusCode.NotFound.code, "not-found") -> StatusCode.NotFound)
          case Some(box) =>
            ZIO.succeed(box)
        }
    }

  protected[backend] val spentBoxesByIds: PublicEndpoint[(Set[String], QueryParams), (ErrorResponse, StatusCode), List[BoxWithAssets], Any] =
    endpoint.post
      .in(rootPath / "boxes" / "spent")
      .in(jsonBody[Set[String]])
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[List[BoxWithAssets]])

  protected[backend] def spentBoxesByIdEndpoint(implicit enc: ErgoAddressEncoder): ZServerEndpoint[BoxService, Any] =
    spentBoxesByIds.zServerLogic { case (boxIds, qp) =>
      BoxService
        .getSpentBoxes(boxIds, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val anyBoxById: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), BoxWithAssets, Any] =
    endpoint.get
      .in(rootPath / "boxes" / "any" / path[String]("boxId"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[BoxWithAssets])

  protected[backend] def anyBoxByIdEndpoint(implicit enc: ErgoAddressEncoder): ZServerEndpoint[BoxService, Any] =
    anyBoxById.zServerLogic { case (boxId, qp) =>
      BoxService
        .getAnyBox(boxId, qp.toMap)
        .mapError(handleThrowable)
        .flatMap {
          case None =>
            ZIO.fail(ErrorResponse(StatusCode.NotFound.code, "not-found") -> StatusCode.NotFound)
          case Some(box) =>
            ZIO.succeed(box)
        }
    }

  protected[backend] val anyBoxesByIds: PublicEndpoint[(Set[String], QueryParams), (ErrorResponse, StatusCode), List[BoxWithAssets], Any] =
    endpoint.post
      .in(rootPath / "boxes" / "any")
      .in(jsonBody[Set[String]])
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[List[BoxWithAssets]])

  protected[backend] def anyBoxesByIdEndpoint(implicit enc: ErgoAddressEncoder): ZServerEndpoint[BoxService, Any] =
    anyBoxesByIds.zServerLogic { case (boxIds, qp) =>
      BoxService
        .getAnyBoxes(boxIds, qp.toMap)
        .mapError(handleThrowable)
    }
