package org.ergoplatform.uexplorer.backend.stats

import org.ergoplatform.uexplorer.backend.{Codecs, ErrorResponse, IdParsingException, TapirRoutes}
import org.ergoplatform.uexplorer.Const
import sttp.model.{QueryParams, StatusCode}
import sttp.tapir.PublicEndpoint
import sttp.tapir.generic.auto.*
import sttp.tapir.json.zio.*
import sttp.tapir.ztapir.*
import zio.*
import zio.json.*

trait StatsTapirRoutes extends TapirRoutes with Codecs:

  protected[backend] val statsTopAddressesByValueEndpoint: PublicEndpoint[QueryParams, (ErrorResponse, StatusCode), String, Any] =
    endpoint.get
      .in(rootPath / "stats" / "top-addresses" / "by-value")
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(stringBody)

  protected[backend] val statsTopAddressesByValueServerEndpoint: ZServerEndpoint[StatsService, Any] =
    statsTopAddressesByValueEndpoint.zServerLogic { qp =>
      val limitStr = qp.toMap.getOrElse("limit", "100000")

      (for
        limit <- ZIO.attempt(limitStr.toInt).mapError(ex => IdParsingException(limitStr, ex.getMessage))
        lines <- StatsService.getTopAddressesByValue(limit, 500)
        response = lines.mkString("", "\n", "\n")
      yield response).mapError(e => ErrorResponse(StatusCode.InternalServerError.code, e.getMessage) -> StatusCode.InternalServerError)
    }

  protected[backend] val statsTopAddressesByUtxoCountEndpoint: PublicEndpoint[QueryParams, (ErrorResponse, StatusCode), String, Any] =
    endpoint.get
      .in(rootPath / "stats" / "top-addresses" / "by-utxo-count")
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(stringBody)

  protected[backend] val statsTopAddressesByUtxoCountServerEndpoint: ZServerEndpoint[StatsService, Any] =
    statsTopAddressesByUtxoCountEndpoint.zServerLogic { qp =>
      val limitStr = qp.toMap.getOrElse("limit", "1000")

      (for
        limit <- ZIO.attempt(limitStr.toInt).mapError(ex => IdParsingException(limitStr, ex.getMessage))
        lines <- StatsService.getTopAddressesByUtxoCount(limit, 500)
        response = lines.mkString("", "\n", "\n")
      yield response).mapError(e => ErrorResponse(StatusCode.InternalServerError.code, e.getMessage) -> StatusCode.InternalServerError)
    }
