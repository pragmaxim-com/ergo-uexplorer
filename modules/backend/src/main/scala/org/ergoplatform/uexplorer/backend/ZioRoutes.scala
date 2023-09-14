package org.ergoplatform.uexplorer.backend

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.backend.blocks.{BlockService, BlockZioRoutes}
import org.ergoplatform.uexplorer.backend.boxes.{BoxService, BoxZioRoutes}
import org.ergoplatform.uexplorer.backend.stats.{StatsService, StatsZioRoutes}
import org.ergoplatform.uexplorer.http.NodePool
import zio.http.*
import zio.json.*
import zio.{Task, ZIO}

trait ZioRoutes:
  val rootPath = "explorer"

  def tapirWithProxyRoutes(implicit enc: ErgoAddressEncoder): App[Client with NodePool with StatsService with BoxService with BlockService] =
    (TapirRoutes.routes ++ ProxyZioRoutes()).withDefaultErrorResponse

  def zioHttpWithProxyRoutes(implicit enc: ErgoAddressEncoder): App[Client with NodePool with StatsService with BoxService with BlockService] =
    (BlockZioRoutes() ++ BoxZioRoutes(enc) ++ StatsZioRoutes() ++ ProxyZioRoutes()).withDefaultErrorResponse

  def throwableToErrorResponse(t: Throwable): Task[Response] = t match {
    case IdParsingException(_, msg) =>
      ZIO.attempt(Response.json(ErrorResponse(Status.BadRequest.code, msg).toJson).withStatus(Status.BadRequest))
    case e: Throwable =>
      ZIO.attempt(Response.json(ErrorResponse(Status.InternalServerError.code, e.getMessage).toJson).withStatus(Status.InternalServerError))
  }
