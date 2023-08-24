package org.ergoplatform.uexplorer.backend.blocks

import org.ergoplatform.uexplorer.BlockId
import org.ergoplatform.uexplorer.backend.Codecs
import org.ergoplatform.uexplorer.db.Block
import zio.*
import zio.http.*
import zio.json.*

object BlockRoutes extends Codecs:

  def apply(): Http[BlockRepo, Throwable, Request, Response] =
    Http.collectZIO[Request] {
      case Method.GET -> Root / "info" =>
        BlockRepo
          .getLastBlocks(1)
          .map(_.headOption)
          .map {
            case Some(lastBlock) =>
              Response.json(Info(lastBlock.height).toJson)
            case None =>
              Response.json(Info(0).toJson)
          }
          .orDie
      case Method.GET -> Root / "blocks" / blockId =>
        BlockRepo
          .lookup(BlockId.fromStringUnsafe(blockId))
          .map {
            case Some(block) =>
              Response.json(block.toJson)
            case None =>
              Response.status(Status.NotFound)
          }
          .orDie
      case req @ Method.POST -> Root / "blocks" =>
        (for {
          u <- req.body.asString.map(_.fromJson[Set[BlockId]])
          r <- u match
                 case Left(e) =>
                   ZIO
                     .debug(s"Failed to parse the input: $e")
                     .as(
                       Response.text(e).withStatus(Status.BadRequest)
                     )
                 case Right(blockIds) =>
                   BlockRepo
                     .lookupBlocks(blockIds)
                     .map(blocks => Response.json(blocks.toJson))
        } yield r).orDie

    }
