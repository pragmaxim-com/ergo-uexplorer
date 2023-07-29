package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.backend.Codecs
import org.ergoplatform.uexplorer.{Address, BoxId, ErgoTreeHash, ErgoTreeHex, ErgoTreeT8Hex}
import zio.*
import zio.http.*
import zio.json.*

object BoxRoutes extends Codecs:
  def apply(): Http[BoxService, Throwable, Request, Response] =
    Http.collectZIO[Request] {
      case Method.GET -> Root / "boxes" / "unspent" / boxId =>
        BoxService
          .getUtxo(BoxId(boxId))
          .map(_.fold(Response.status(Status.NotFound))(box => Response.json(box.toJson)))
          .orDie

      case req @ Method.POST -> Root / "boxes" / "unspent" =>
        (for {
          u <- req.body.asString.map(_.fromJson[Set[BoxId]])
          r <- u.fold(
                 e => ZIO.succeed(Response.text(e).withStatus(Status.BadRequest)),
                 boxIds => BoxService.getUtxos(boxIds).map(utxos => Response.json(utxos.toJson))
               )
        } yield r).orDie

      case Method.GET -> Root / "boxes" / "spent" / boxId =>
        BoxService
          .getSpentBox(BoxId(boxId))
          .map(_.fold(Response.status(Status.NotFound))(box => Response.json(box.toJson)))
          .orDie

      case req @ Method.POST -> Root / "boxes" / "spent" =>
        (for {
          u <- req.body.asString.map(_.fromJson[Set[BoxId]])
          r <- u.fold(
                 e => ZIO.succeed(Response.text(e).withStatus(Status.BadRequest)),
                 boxIds => BoxService.getSpentBoxes(boxIds).map(spentBoxes => Response.json(spentBoxes.toJson))
               )
        } yield r).orDie

      case Method.GET -> Root / "boxes" / "any" / boxId =>
        BoxService
          .getAnyBox(BoxId(boxId))
          .map(_.fold(Response.status(Status.NotFound))(box => Response.json(box.toJson)))
          .orDie

      case req @ Method.POST -> Root / "boxes" / "any" =>
        (for {
          u <- req.body.asString.map(_.fromJson[Set[BoxId]])
          r <- u.fold(
                 e => ZIO.succeed(Response.text(e).withStatus(Status.BadRequest)),
                 boxIds => BoxService.getAnyBoxes(boxIds).map(utxos => Response.json(utxos.toJson))
               )
        } yield r).orDie

      case req @ (Method.GET -> Root / "boxes" / "spent" / "by-address" / address) =>
        BoxService
          .getSpentBoxesByAddress(Address.fromStringUnsafe(address), req.url.queryParams)
          .map(boxes => Response.json(boxes.toJson))
          .orDie

      case req @ (Method.GET -> Root / "boxes" / "unspent" / "by-address" / address) =>
        BoxService
          .getUnspentBoxesByAddress(Address.fromStringUnsafe(address), req.url.queryParams)
          .map(utxos => Response.json(utxos.toJson))
          .orDie

      case req @ (Method.GET -> Root / "boxes" / "any" / "by-address" / address) =>
        BoxService
          .getAnyBoxesByAddress(Address.fromStringUnsafe(address), req.url.queryParams)
          .map(boxes => Response.json(boxes.toJson))
          .orDie

      case req @ (Method.GET -> Root / "boxes" / "spent" / "contracts" / "by-ergo-tree" / ergoTree) =>
        BoxService
          .getSpentBoxesByErgoTree(ErgoTreeHex.fromStringUnsafe(ergoTree), req.url.queryParams)
          .map(boxes => Response.json(boxes.toJson))
          .orDie

      case req @ (Method.GET -> Root / "boxes" / "unspent" / "contracts" / "by-ergo-tree" / ergoTree) =>
        BoxService
          .getUnspentBoxesByErgoTree(ErgoTreeHex.fromStringUnsafe(ergoTree), req.url.queryParams)
          .map(utxos => Response.json(utxos.toJson))
          .orDie

      case req @ (Method.GET -> Root / "boxes" / "any" / "contracts" / "by-ergo-tree" / ergoTree) =>
        BoxService
          .getAnyBoxesByErgoTree(ErgoTreeHex.fromStringUnsafe(ergoTree), req.url.queryParams)
          .map(boxes => Response.json(boxes.toJson))
          .orDie

      case req @ (Method.GET -> Root / "boxes" / "spent" / "contracts" / "by-ergo-tree-hash" / ergoTreeHash) =>
        BoxService
          .getSpentBoxesByErgoTreeHash(ErgoTreeHash.fromStringUnsafe(ergoTreeHash), req.url.queryParams)
          .map(boxes => Response.json(boxes.toJson))
          .orDie

      case req @ (Method.GET -> Root / "boxes" / "unspent" / "contracts" / "by-ergo-tree-hash" / ergoTreeHash) =>
        BoxService
          .getUnspentBoxesByErgoTreeHash(ErgoTreeHash.fromStringUnsafe(ergoTreeHash), req.url.queryParams)
          .map(utxos => Response.json(utxos.toJson))
          .orDie

      case req @ (Method.GET -> Root / "boxes" / "any" / "contracts" / "by-ergo-tree-hash" / ergoTreeHash) =>
        BoxService
          .getAnyBoxesByErgoTreeHash(ErgoTreeHash.fromStringUnsafe(ergoTreeHash), req.url.queryParams)
          .map(boxes => Response.json(boxes.toJson))
          .orDie

      case req @ (Method.GET -> Root / "boxes" / "spent" / "templates" / "by-ergo-tree" / ergoTreeT8) =>
        BoxService
          .getSpentBoxesByErgoTreeT8(ErgoTreeT8Hex.fromStringUnsafe(ergoTreeT8), req.url.queryParams)
          .map(boxes => Response.json(boxes.toJson))
          .orDie

      case req @ (Method.GET -> Root / "boxes" / "unspent" / "templates" / "by-ergo-tree" / ergoTreeT8) =>
        BoxService
          .getUnspentBoxesByErgoTreeT8(ErgoTreeT8Hex.fromStringUnsafe(ergoTreeT8), req.url.queryParams)
          .map(utxos => Response.json(utxos.toJson))
          .orDie

      case req @ (Method.GET -> Root / "boxes" / "any" / "templates" / "by-ergo-tree" / ergoTreeT8) =>
        BoxService
          .getAnyBoxesByErgoTreeT8(ErgoTreeT8Hex.fromStringUnsafe(ergoTreeT8), req.url.queryParams)
          .map(boxes => Response.json(boxes.toJson))
          .orDie

      case req @ (Method.GET -> Root / "boxes" / "spent" / "templates" / "by-ergo-tree-hash" / ergoTreeT8Hash) =>
        BoxService
          .getSpentBoxesByErgoTreeT8Hash(ErgoTreeHash.fromStringUnsafe(ergoTreeT8Hash), req.url.queryParams)
          .map(boxes => Response.json(boxes.toJson))
          .orDie

      case req @ (Method.GET -> Root / "boxes" / "unspent" / "templates" / "by-ergo-tree-hash" / ergoTreeT8Hash) =>
        BoxService
          .getUnspentBoxesByErgoTreeT8Hash(ErgoTreeHash.fromStringUnsafe(ergoTreeT8Hash), req.url.queryParams)
          .map(utxos => Response.json(utxos.toJson))
          .orDie

      case req @ (Method.GET -> Root / "boxes" / "any" / "templates" / "by-ergo-tree-hash" / ergoTreeT8Hash) =>
        BoxService
          .getAnyBoxesByErgoTreeT8Hash(ErgoTreeHash.fromStringUnsafe(ergoTreeT8Hash), req.url.queryParams)
          .map(boxes => Response.json(boxes.toJson))
          .orDie

    }
