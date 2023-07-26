package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.backend.Codecs
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId, ErgoTreeHash, ErgoTreeHex, ErgoTreeT8Hex}
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

      case Method.GET -> Root / "boxes" / "spent" / "addresses" / address =>
        BoxService
          .getSpentBoxesByAddress(Address.fromStringUnsafe(address))
          .map(boxes => Response.json(boxes.toJson))
          .orDie

      case Method.GET -> Root / "boxes" / "unspent" / "addresses" / address =>
        BoxService
          .getUnspentBoxesByAddress(Address.fromStringUnsafe(address))
          .map(utxos => Response.json(utxos.toJson))
          .orDie

      case Method.GET -> Root / "boxes" / "any" / "addresses" / address =>
        BoxService
          .getAnyBoxesByAddress(Address.fromStringUnsafe(address))
          .map(boxes => Response.json(boxes.toJson))
          .orDie

      case Method.GET -> Root / "boxes" / "spent" / "contracts" / "ergo-trees" / ergoTree =>
        BoxService
          .getSpentBoxesByErgoTree(ErgoTreeHex.fromStringUnsafe(ergoTree))
          .map(boxes => Response.json(boxes.toJson))
          .orDie

      case Method.GET -> Root / "boxes" / "unspent" / "contracts" / "ergo-trees" / ergoTree =>
        BoxService
          .getUnspentBoxesByErgoTree(ErgoTreeHex.fromStringUnsafe(ergoTree))
          .map(utxos => Response.json(utxos.toJson))
          .orDie

      case Method.GET -> Root / "boxes" / "any" / "contracts" / "ergo-trees" / ergoTree =>
        BoxService
          .getAnyBoxesByErgoTree(ErgoTreeHex.fromStringUnsafe(ergoTree))
          .map(boxes => Response.json(boxes.toJson))
          .orDie

      case Method.GET -> Root / "boxes" / "spent" / "contracts" / "ergo-tree-hashes" / ergoTreeHash =>
        BoxService
          .getSpentBoxesByErgoTreeHash(ErgoTreeHash.fromStringUnsafe(ergoTreeHash))
          .map(boxes => Response.json(boxes.toJson))
          .orDie

      case Method.GET -> Root / "boxes" / "unspent" / "contracts" / "ergo-tree-hashes" / ergoTreeHash =>
        BoxService
          .getUnspentBoxesByErgoTreeHash(ErgoTreeHash.fromStringUnsafe(ergoTreeHash))
          .map(utxos => Response.json(utxos.toJson))
          .orDie

      case Method.GET -> Root / "boxes" / "any" / "contracts" / "ergo-tree-hashes" / ergoTreeHash =>
        BoxService
          .getAnyBoxesByErgoTreeHash(ErgoTreeHash.fromStringUnsafe(ergoTreeHash))
          .map(boxes => Response.json(boxes.toJson))
          .orDie

      case Method.GET -> Root / "boxes" / "spent" / "templates" / "ergo-trees" / ergoTreeT8 =>
        BoxService
          .getSpentBoxesByErgoTreeT8(ErgoTreeT8Hex.fromStringUnsafe(ergoTreeT8))
          .map(boxes => Response.json(boxes.toJson))
          .orDie

      case Method.GET -> Root / "boxes" / "unspent" / "templates" / "ergo-trees" / ergoTreeT8 =>
        BoxService
          .getUnspentBoxesByErgoTreeT8(ErgoTreeT8Hex.fromStringUnsafe(ergoTreeT8))
          .map(utxos => Response.json(utxos.toJson))
          .orDie

      case Method.GET -> Root / "boxes" / "any" / "templates" / "ergo-trees" / ergoTreeT8 =>
        BoxService
          .getAnyBoxesByErgoTreeT8(ErgoTreeT8Hex.fromStringUnsafe(ergoTreeT8))
          .map(boxes => Response.json(boxes.toJson))
          .orDie

      case Method.GET -> Root / "boxes" / "spent" / "templates" / "ergo-tree-hashes" / ergoTreeT8Hash =>
        BoxService
          .getSpentBoxesByErgoTreeT8Hash(ErgoTreeHash.fromStringUnsafe(ergoTreeT8Hash))
          .map(boxes => Response.json(boxes.toJson))
          .orDie

      case Method.GET -> Root / "boxes" / "unspent" / "templates" / "ergo-tree-hashes" / ergoTreeT8Hash =>
        BoxService
          .getUnspentBoxesByErgoTreeT8Hash(ErgoTreeHash.fromStringUnsafe(ergoTreeT8Hash))
          .map(utxos => Response.json(utxos.toJson))
          .orDie

      case Method.GET -> Root / "boxes" / "any" / "templates" / "ergo-tree-hashes" / ergoTreeT8Hash =>
        BoxService
          .getAnyBoxesByErgoTreeT8Hash(ErgoTreeHash.fromStringUnsafe(ergoTreeT8Hash))
          .map(boxes => Response.json(boxes.toJson))
          .orDie

    }
