package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.backend.Codecs
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId}
import zio.*
import zio.http.*
import zio.json.*

object BoxRoutes extends Codecs:
  def apply(): Http[BoxRepo, Throwable, Request, Response] =
    Http.collectZIO[Request] {
      case Method.GET -> Root / "boxes" / "unspent" / boxId =>
        BoxRepo
          .lookupUtxo(BoxId(boxId))
          .map {
            case Some(box) =>
              Response.json(box.toJson)
            case None =>
              Response.status(Status.NotFound)
          }
          .orDie

      case req @ Method.POST -> Root / "boxes" / "unspent" =>
        (for {
          u <- req.body.asString.map(_.fromJson[Set[BoxId]])
          r <- u match
                 case Left(e) =>
                   ZIO
                     .debug(s"Failed to parse the input: $e")
                     .as(
                       Response.text(e).withStatus(Status.BadRequest)
                     )
                 case Right(boxIds) =>
                   BoxRepo
                     .lookupUtxos(boxIds)
                     .map(utxos => Response.json(utxos.toJson))
        } yield r).orDie

      case Method.GET -> Root / "boxes" / "spent" / boxId =>
        BoxRepo
          .lookupUtxo(BoxId(boxId))
          .flatMap {
            case Some(_) =>
              ZIO.succeed(Response.status(Status.NotFound))
            case None =>
              BoxRepo
                .lookupBox(BoxId(boxId))
                .map {
                  case Some(box) =>
                    Response.json(box.toJson)
                  case None =>
                    Response.status(Status.NotFound)
                }
          }
          .orDie

      case req @ Method.POST -> Root / "boxes" / "spent" =>
        (for {
          u <- req.body.asString.map(_.fromJson[Set[BoxId]])
          r <- u match
                 case Left(e) =>
                   ZIO
                     .debug(s"Failed to parse the input: $e")
                     .as(
                       Response.text(e).withStatus(Status.BadRequest)
                     )
                 case Right(boxIds) =>
                   BoxRepo
                     .lookupUtxos(boxIds)
                     .flatMap { utxos =>
                       val utxoIds = utxos.map(_.boxId)
                       BoxRepo
                         .lookupBoxes(boxIds)
                         .map(allBoxes => allBoxes.filter(b => !utxoIds.contains(b.boxId)))
                         .map(spentBoxes => Response.json(spentBoxes.toJson))
                     }
        } yield r).orDie

      /*
      case Method.GET -> Root / "addresses" / address =>
        BoxRepo
          .lookupUtxo(Address.fromStringUnsafe(address))
          .flatMap {
            case Some(_) =>
              ZIO.succeed(Response.status(Status.NotFound))
            case None =>
              BoxRepo
                .lookupBox(BoxId(boxId))
                .map {
                  case Some(box) =>
                    Response.json(box.toJson)
                  case None =>
                    Response.status(Status.NotFound)
                }
          }
          .orDie
       */

    }
