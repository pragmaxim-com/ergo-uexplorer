package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.BlockId.unwrapped
import org.ergoplatform.uexplorer.BoxId.unwrapped
import org.ergoplatform.uexplorer.backend.Codecs
import org.ergoplatform.uexplorer.db.{Asset2Box, Block, Box, Utxo}
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId, TxId}
import sttp.model.QueryParams
import sttp.tapir.generic.auto.*
import sttp.tapir.json.zio.*
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.swagger.bundle.SwaggerInterpreter
import sttp.tapir.ztapir.*
import sttp.tapir.{queryParams, PublicEndpoint, Schema}
import zio.http.{HttpApp, Server}
import zio.json.*
import zio.*

trait BoxesByIdRoutes extends Codecs:

  protected[backend] val unspentBoxById: PublicEndpoint[BoxId, String, Option[Utxo], Any] =
    endpoint.get
      .in("boxes" / "unspent" / path[String]("boxId"))
      .mapIn(BoxId(_))(_.unwrapped)
      .errorOut(stringBody)
      .out(jsonBody[Option[Utxo]])

  protected[backend] val unspentBoxByIdEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentBoxById.zServerLogic { boxId =>
      BoxService
        .getUtxo(boxId)
        .mapError(_.getMessage)
    }

  protected[backend] val unspentBoxesByIds: PublicEndpoint[Set[BoxId], String, List[Utxo], Any] =
    endpoint.post
      .in("boxes" / "unspent")
      .in(jsonBody[Set[BoxId]])
      .errorOut(stringBody)
      .out(jsonBody[List[Utxo]])

  protected[backend] val unspentBoxesByIdEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentBoxesByIds.zServerLogic { boxIds =>
      BoxService
        .getUtxos(boxIds)
        .mapError(_.getMessage)
    }

  protected[backend] val spentBoxById: PublicEndpoint[BoxId, String, Option[Box], Any] =
    endpoint.get
      .in("boxes" / "spent" / path[String]("boxId"))
      .mapIn(BoxId(_))(_.unwrapped)
      .errorOut(stringBody)
      .out(jsonBody[Option[Box]])

  protected[backend] val spentBoxByIdEndpoint: ZServerEndpoint[BoxService, Any] =
    spentBoxById.zServerLogic { boxId =>
      BoxService
        .getSpentBox(boxId)
        .mapError(_.getMessage)
    }

  protected[backend] val spentBoxesByIds: PublicEndpoint[Set[BoxId], String, List[Box], Any] =
    endpoint.post
      .in("boxes" / "spent")
      .in(jsonBody[Set[BoxId]])
      .errorOut(stringBody)
      .out(jsonBody[List[Box]])

  protected[backend] val spentBoxesByIdEndpoint: ZServerEndpoint[BoxService, Any] =
    spentBoxesByIds.zServerLogic { boxIds =>
      BoxService
        .getSpentBoxes(boxIds)
        .mapError(_.getMessage)
    }

  protected[backend] val anyBoxById: PublicEndpoint[BoxId, String, Option[Box], Any] =
    endpoint.get
      .in("boxes" / "any" / path[String]("boxId"))
      .mapIn(BoxId(_))(_.unwrapped)
      .errorOut(stringBody)
      .out(jsonBody[Option[Box]])

  protected[backend] val anyBoxByIdEndpoint: ZServerEndpoint[BoxService, Any] =
    anyBoxById.zServerLogic { boxId =>
      BoxService
        .getAnyBox(boxId)
        .mapError(_.getMessage)
    }

  protected[backend] val anyBoxesByIds: PublicEndpoint[Set[BoxId], String, List[Box], Any] =
    endpoint.post
      .in("boxes" / "any")
      .in(jsonBody[Set[BoxId]])
      .errorOut(stringBody)
      .out(jsonBody[List[Box]])

  protected[backend] val anyBoxesByIdEndpoint: ZServerEndpoint[BoxService, Any] =
    anyBoxesByIds.zServerLogic { boxIds =>
      BoxService
        .getAnyBoxes(boxIds)
        .mapError(_.getMessage)
    }
