package org.ergoplatform.uexplorer.backend.blocks

import org.ergoplatform.uexplorer.backend.{Codecs, ErrorResponse, IdParsingException, ZioRoutes}
import org.ergoplatform.uexplorer.db.Block
import zio.*
import zio.http.*
import zio.json.*

object BlockZioRoutes extends ZioRoutes with Codecs:

  def apply(): Http[BlockService, Throwable, Request, Response] =
    Http.collectZIO[Request] {
      case Method.GET -> Root / rootPath / "info" =>
        BlockService
          .getLastBlocks(1)
          .map(_.headOption)
          .map {
            case Some(lastBlock) =>
              Response.json(Info(lastBlock.height).toJson)
            case None =>
              Response.json(Info(0).toJson)
          }
          .catchAllDefect { case e: Throwable =>
            ZIO.attempt(Response.json(ErrorResponse(Status.InternalServerError.code, e.getMessage).toJson).withStatus(Status.InternalServerError))
          }
          .orDie
      case Method.GET -> Root / rootPath / "blocks" / blockId =>
        BlockService
          .lookup(blockId)
          .map {
            case Some(block) =>
              Response.json(block.toJson)
            case None =>
              Response.json(ErrorResponse(Status.NotFound.code, "not-found").toJson).withStatus(Status.NotFound)
          }
          .catchAllDefect {
            case IdParsingException(_, msg) =>
              ZIO.attempt(Response.json(ErrorResponse(Status.BadRequest.code, msg).toJson).withStatus(Status.BadRequest))
            case e: Throwable =>
              ZIO.attempt(Response.json(ErrorResponse(Status.InternalServerError.code, e.getMessage).toJson).withStatus(Status.InternalServerError))
          }
          .orDie
      case req @ Method.POST -> Root / rootPath / "blocks" =>
        (for {
          u <- req.body.asString.map(_.fromJson[Set[String]])
          r <- u match
                 case Left(e) =>
                   ZIO.attempt(Response.json(ErrorResponse(Status.BadRequest.code, e).toJson).withStatus(Status.BadRequest))
                 case Right(blockIds) =>
                   BlockService
                     .lookupBlocks(blockIds)
                     .map(blocks => Response.json(blocks.toJson))
                     .catchAllDefect {
                       case IdParsingException(_, msg) =>
                         ZIO.attempt(Response.json(ErrorResponse(Status.BadRequest.code, msg).toJson).withStatus(Status.BadRequest))
                       case e: Throwable =>
                         ZIO.attempt(Response.json(ErrorResponse(Status.InternalServerError.code, e.getMessage).toJson).withStatus(Status.InternalServerError))
                     }
        } yield r).orDie

    }
