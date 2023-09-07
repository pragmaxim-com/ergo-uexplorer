package org.ergoplatform.uexplorer.backend

import org.ergoplatform.uexplorer.CoreConf
import org.ergoplatform.uexplorer.backend.blocks.{BlockRoutes, BlockService, PersistentBlockRepo}
import org.ergoplatform.uexplorer.backend.boxes.{BoxRoutes, BoxService, PersistentBoxRepo}
import org.ergoplatform.uexplorer.http.{NodePool, NodePoolConf}
import zio.{Task, ZIO}
import zio.http.*
import zio.json.*

trait ZioRoutes:
  val rootPath = "explorer"

  val tapirWithProxyRoutes: App[Client with NodePool with BoxService with BlockService] =
    (TapirRoutes.routes ++ ProxyZioRoutes()).withDefaultErrorResponse

  val zioHttpWithProxyRoutes: App[Client with NodePool with BoxService with BlockService] =
    (BlockRoutes() ++ BoxRoutes() ++ ProxyZioRoutes()).withDefaultErrorResponse

  val routeLayers =
    Client.default >+>
      CoreConf.layer >+>
      NodePoolConf.layer >+>
      NodePool.layerInitializedFromConf >+>
      H2Backend.zLayerFromConf >+>
      PersistentBoxRepo.layer >+>
      PersistentBlockRepo.layer >+>
      BoxService.layer >+>
      BlockService.layer

  def throwableToErrorResponse(t: Throwable): Task[Response] = t match {
    case IdParsingException(_, msg) =>
      ZIO.attempt(Response.json(ErrorResponse(Status.BadRequest.code, msg).toJson).withStatus(Status.BadRequest))
    case e: Throwable =>
      ZIO.attempt(Response.json(ErrorResponse(Status.InternalServerError.code, e.getMessage).toJson).withStatus(Status.InternalServerError))
  }
