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

trait BoxesByAddress extends Codecs:

  protected[backend] val spentBoxesByAddress: PublicEndpoint[(String, QueryParams), String, Iterable[Box], Any] =
    endpoint.get
      .in("boxes" / "spent" / "by-address" / path[String]("address"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[Box]])

  protected[backend] val spentBoxesByAddressEndpoint: ZServerEndpoint[BoxService, Any] =
    spentBoxesByAddress.zServerLogic { case (address, qp) =>
      BoxService
        .getSpentBoxesByAddress(address, qp.toMap)
        .mapError(_.getMessage)
    }

  protected[backend] val spentBoxIdsByAddress: PublicEndpoint[(String, QueryParams), String, Iterable[BoxId], Any] =
    endpoint.get
      .in("box-ids" / "spent" / "by-address" / path[String]("address"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[BoxId]])

  protected[backend] val spentBoxIdsByAddressEndpoint: ZServerEndpoint[BoxService, Any] =
    spentBoxIdsByAddress.zServerLogic { case (address, qp) =>
      BoxService
        .getSpentBoxesByAddress(address, qp.toMap)
        .mapBoth(_.getMessage, _.map(_.boxId))
    }

  protected[backend] val unspentBoxesByAddress: PublicEndpoint[(String, QueryParams), String, Iterable[Utxo], Any] =
    endpoint.get
      .in("boxes" / "unspent" / "by-address" / path[String]("address"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[Utxo]])

  protected[backend] val unspentBoxesByAddressEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentBoxesByAddress.zServerLogic { case (address, qp) =>
      BoxService
        .getUnspentBoxesByAddress(address, qp.toMap)
        .mapError(_.getMessage)
    }

  protected[backend] val unspentBoxIdsByAddress: PublicEndpoint[(String, QueryParams), String, Iterable[BoxId], Any] =
    endpoint.get
      .in("box-ids" / "unspent" / "by-address" / path[String]("address"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[BoxId]])

  protected[backend] val unspentBoxIdsByAddressEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentBoxIdsByAddress.zServerLogic { case (address, qp) =>
      BoxService
        .getUnspentBoxesByAddress(address, qp.toMap)
        .mapBoth(_.getMessage, _.map(_.boxId))
    }

  protected[backend] val anyBoxesByAddress: PublicEndpoint[(String, QueryParams), String, Iterable[Box], Any] =
    endpoint.get
      .in("boxes" / "any" / "by-address" / path[String]("address"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[Box]])

  protected[backend] val anyBoxesByAddressEndpoint: ZServerEndpoint[BoxService, Any] =
    anyBoxesByAddress.zServerLogic { case (address, qp) =>
      BoxService
        .getAnyBoxesByAddress(address, qp.toMap)
        .mapError(_.getMessage)
    }

  protected[backend] val anyBoxIdsByAddress: PublicEndpoint[(String, QueryParams), String, Iterable[BoxId], Any] =
    endpoint.get
      .in("box-ids" / "any" / "by-address" / path[String]("address"))
      .in(queryParams)
      .errorOut(stringBody)
      .out(jsonBody[Iterable[BoxId]])

  protected[backend] val anyBoxIdsByAddressEndpoint: ZServerEndpoint[BoxService, Any] =
    anyBoxIdsByAddress.zServerLogic { case (address, qp) =>
      BoxService
        .getAnyBoxesByAddress(address, qp.toMap)
        .mapBoth(_.getMessage, _.map(_.boxId))
    }
