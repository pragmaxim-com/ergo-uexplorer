package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.backend.{Codecs, ErrorResponse, TapirRoutes}
import org.ergoplatform.uexplorer.db.{Box, Utxo}
import org.ergoplatform.uexplorer.{BlockId, BoxId, TxId}
import sttp.model.StatusCode
import sttp.tapir.generic.auto.*
import sttp.tapir.json.zio.*
import sttp.tapir.ztapir.*
import sttp.tapir.{PublicEndpoint, queryParams}
import zio.*
import zio.json.*

trait BoxesByIdRoutes extends TapirRoutes with Codecs:

  protected[backend] val unspentBoxById: PublicEndpoint[String, (ErrorResponse, StatusCode), Utxo, Any] =
    endpoint.get
      .in(rootPath / "boxes" / "unspent" / path[String]("boxId"))
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Utxo])
      .description("Get unspent box by box ID")

  protected[backend] val unspentBoxByIdEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentBoxById.zServerLogic { boxId =>
      BoxService
        .getUtxo(boxId)
        .mapError(handleThrowable)
        .flatMap {
          case None =>
            ZIO.fail(ErrorResponse(StatusCode.NotFound.code, "not-found") -> StatusCode.NotFound)
          case Some(utxo) =>
            ZIO.succeed(utxo)
        }
    }

  protected[backend] val unspentBoxesByIds: PublicEndpoint[Set[String], (ErrorResponse, StatusCode), List[Utxo], Any] =
    endpoint.post
      .in(rootPath / "boxes" / "unspent")
      .in(jsonBody[Set[String]])
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[List[Utxo]])

  protected[backend] val unspentBoxesByIdEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentBoxesByIds.zServerLogic { boxIds =>
      BoxService
        .getUtxos(boxIds)
        .mapError(handleThrowable)
    }

  protected[backend] val spentBoxById: PublicEndpoint[String, (ErrorResponse, StatusCode), Box, Any] =
    endpoint.get
      .in(rootPath / "boxes" / "spent" / path[String]("boxId"))
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Box])

  protected[backend] val spentBoxByIdEndpoint: ZServerEndpoint[BoxService, Any] =
    spentBoxById.zServerLogic { boxId =>
      BoxService
        .getSpentBox(boxId)
        .mapError(handleThrowable)
        .flatMap {
          case None =>
            ZIO.fail(ErrorResponse(StatusCode.NotFound.code, "not-found") -> StatusCode.NotFound)
          case Some(box) =>
            ZIO.succeed(box)
        }

    }

  protected[backend] val spentBoxesByIds: PublicEndpoint[Set[String], (ErrorResponse, StatusCode), List[Box], Any] =
    endpoint.post
      .in(rootPath / "boxes" / "spent")
      .in(jsonBody[Set[String]])
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[List[Box]])

  protected[backend] val spentBoxesByIdEndpoint: ZServerEndpoint[BoxService, Any] =
    spentBoxesByIds.zServerLogic { boxIds =>
      BoxService
        .getSpentBoxes(boxIds)
        .mapError(handleThrowable)
    }

  protected[backend] val anyBoxById: PublicEndpoint[String, (ErrorResponse, StatusCode), Box, Any] =
    endpoint.get
      .in(rootPath / "boxes" / "any" / path[String]("boxId"))
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Box])

  protected[backend] val anyBoxByIdEndpoint: ZServerEndpoint[BoxService, Any] =
    anyBoxById.zServerLogic { boxId =>
      BoxService
        .getAnyBox(boxId)
        .mapError(handleThrowable)
        .flatMap {
          case None =>
            ZIO.fail(ErrorResponse(StatusCode.NotFound.code, "not-found") -> StatusCode.NotFound)
          case Some(box) =>
            ZIO.succeed(box)
        }
    }

  protected[backend] val anyBoxesByIds: PublicEndpoint[Set[String], (ErrorResponse, StatusCode), List[Box], Any] =
    endpoint.post
      .in(rootPath / "boxes" / "any")
      .in(jsonBody[Set[String]])
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[List[Box]])

  protected[backend] val anyBoxesByIdEndpoint: ZServerEndpoint[BoxService, Any] =
    anyBoxesByIds.zServerLogic { boxIds =>
      BoxService
        .getAnyBoxes(boxIds)
        .mapError(handleThrowable)
    }
