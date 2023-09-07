package org.ergoplatform.uexplorer.backend.blocks

import org.ergoplatform.uexplorer.{Address, BlockId}
import org.ergoplatform.uexplorer.db.Block
import zio._
import sttp.tapir.{PublicEndpoint, Schema}
import sttp.tapir.generic.auto.*
import sttp.tapir.ztapir.*
import sttp.tapir.json.zio.*
import org.ergoplatform.uexplorer.backend.{Codecs, ErrorResponse}
import sttp.model.StatusCode
import zio.json.{JsonDecoder, JsonEncoder}

trait BlockTapirRoutes extends Codecs:

  protected[backend] val infoEndpoint: PublicEndpoint[Unit, (ErrorResponse, StatusCode), Info, Any] =
    endpoint.get
      .in("info")
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Info])

  protected[backend] val infoServerEndpoint: ZServerEndpoint[BlockService, Any] =
    infoEndpoint.zServerLogic { _ =>
      BlockService
        .getLastBlocks(1)
        .mapError(e => ErrorResponse(StatusCode.InternalServerError.code, e.getMessage) -> StatusCode.InternalServerError)
        .map(_.headOption)
        .map {
          case Some(lastBlock) =>
            Info(lastBlock.height)
          case None =>
            Info(0)
        }

    }

  protected[backend] val blockByIdEndpoint: PublicEndpoint[String, (ErrorResponse, StatusCode), Block, Any] =
    endpoint.get
      .in("blocks" / path[String]("blockId"))
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Block])

  protected[backend] val blockByIdServerEndpoint: ZServerEndpoint[BlockService, Any] =
    blockByIdEndpoint.zServerLogic { blockId =>
      BlockService
        .lookup(blockId)
        .mapError(e => ErrorResponse(StatusCode.InternalServerError.code, e.getMessage) -> StatusCode.InternalServerError)
        .flatMap {
          case None =>
            ZIO.fail(ErrorResponse(StatusCode.NotFound.code, "not-found") -> StatusCode.NotFound)
          case Some(block) =>
            ZIO.succeed(block)
        }
    }

  protected[backend] val blockByIdsEndpoint: PublicEndpoint[Set[String], (ErrorResponse, StatusCode), List[Block], Any] =
    endpoint.post
      .in("blocks")
      .in(jsonBody[Set[String]])
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[List[Block]])

  protected[backend] val blockByIdsServerEndpoint: ZServerEndpoint[BlockService, Any] =
    blockByIdsEndpoint.zServerLogic { blockIds =>
      BlockService
        .lookupBlocks(blockIds)
        .mapError(e => ErrorResponse(StatusCode.InternalServerError.code, e.getMessage) -> StatusCode.InternalServerError)
    }
