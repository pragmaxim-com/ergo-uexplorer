package org.ergoplatform.uexplorer.backend.stats

import org.ergoplatform.uexplorer.Const
import org.ergoplatform.uexplorer.backend.{Codecs, IdParsingException, ZioRoutes}
import zio.*
import zio.http.*
import zio.json.*

object StatsZioRoutes extends ZioRoutes with Codecs:

  def apply(): Http[StatsService, Throwable, Request, Response] =
    Http.collectZIO[Request] {
      case req @ Method.GET -> Root / rootPath / "stats" / "top-addresses" / "by-value" =>
        val limitStr = req.url.queryParams.map.view.mapValues(_.head).toMap.getOrElse("limit", "100000")

        (for
          limit <- ZIO.attempt(limitStr.toInt).mapError(ex => IdParsingException(limitStr, ex.getMessage))
          lines <- StatsService.getTopAddressesByValue(limit, 500)
          response = Response.text(lines.mkString("", "\n", "\n"))
        yield response).catchAllDefect(throwableToErrorResponse).orDie

      case req @ Method.GET -> Root / rootPath / "stats" / "top-addresses" / "by-utxo-count" =>
        val limitStr = req.url.queryParams.map.view.mapValues(_.head).toMap.getOrElse("limit", "1000")

        (for
          limit <- ZIO.attempt(limitStr.toInt).mapError(ex => IdParsingException(limitStr, ex.getMessage))
          lines <- StatsService.getTopAddressesByUtxoCount(limit, 500)
          response = Response.text(lines.mkString("", "\n", "\n"))
        yield response).catchAllDefect(throwableToErrorResponse).orDie
    }
