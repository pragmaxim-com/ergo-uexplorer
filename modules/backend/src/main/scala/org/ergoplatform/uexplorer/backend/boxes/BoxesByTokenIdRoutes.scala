package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.backend.{Codecs, ErrorResponse, TapirRoutes}
import org.ergoplatform.uexplorer.db.{Asset2Box, Box, Utxo}
import org.ergoplatform.uexplorer.{BoxId, TxId}
import sttp.model.{QueryParams, StatusCode}
import sttp.tapir.generic.auto.*
import sttp.tapir.json.zio.*
import sttp.tapir.ztapir.*
import sttp.tapir.{queryParams, PublicEndpoint, Schema}
import zio.*
import zio.json.*

trait BoxesByTokenIdRoutes extends TapirRoutes with Codecs:

  protected[backend] val unspentAssetsByTokenId: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[Asset2Box], Any] =
    endpoint.get
      .in(rootPath / "assets" / "unspent" / "by-token-id" / path[String]("tokenId"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[Asset2Box]])

  protected[backend] val unspentAssetsByTokenIdEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentAssetsByTokenId.zServerLogic { case (tokenId, qp) =>
      BoxService
        .getUnspentAssetsByTokenId(tokenId, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val spentAssetsByTokenId: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[Asset2Box], Any] =
    endpoint.get
      .in(rootPath / "assets" / "spent" / "by-token-id" / path[String]("tokenId"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[Asset2Box]])

  protected[backend] val spentAssetsByTokenIdEndpoint: ZServerEndpoint[BoxService, Any] =
    spentAssetsByTokenId.zServerLogic { case (tokenId, qp) =>
      BoxService
        .getSpentAssetsByTokenId(tokenId, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val anyAssetsByTokenId: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[Asset2Box], Any] =
    endpoint.get
      .in(rootPath / "assets" / "any" / "by-token-id" / path[String]("tokenId"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[Asset2Box]])

  protected[backend] val anyAssetsByTokenIdEndpoint: ZServerEndpoint[BoxService, Any] =
    anyAssetsByTokenId.zServerLogic { case (tokenId, qp) =>
      BoxService
        .getAnyAssetsByTokenId(tokenId, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val unspentBoxesByTokenId: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[Utxo], Any] =
    endpoint.get
      .in(rootPath / "boxes" / "unspent" / "by-token-id" / path[String]("tokenId"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[Utxo]])

  protected[backend] val unspentBoxesByTokenIdEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentBoxesByTokenId.zServerLogic { case (tokenId, qp) =>
      BoxService
        .getUnspentBoxesByTokenId(tokenId, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val unspentBoxIdsByTokenId: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxId], Any] =
    endpoint.get
      .in(rootPath / "box-ids" / "unspent" / "by-token-id" / path[String]("tokenId"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxId]])

  protected[backend] val unspentBoxIdsByTokenIdEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentBoxIdsByTokenId.zServerLogic { case (tokenId, qp) =>
      BoxService
        .getUnspentBoxesByTokenId(tokenId, qp.toMap)
        .map(_.map(_.boxId))
        .mapError(handleThrowable)
    }

  protected[backend] val spentBoxesByTokenId: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[Box], Any] =
    endpoint.get
      .in(rootPath / "boxes" / "spent" / "by-token-id" / path[String]("tokenId"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[Box]])

  protected[backend] val spentBoxesByTokenIdEndpoint: ZServerEndpoint[BoxService, Any] =
    spentBoxesByTokenId.zServerLogic { case (tokenId, qp) =>
      BoxService
        .getSpentBoxesByTokenId(tokenId, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val spentBoxIdsByTokenId: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxId], Any] =
    endpoint.get
      .in(rootPath / "box-ids" / "spent" / "by-token-id" / path[String]("tokenId"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxId]])

  protected[backend] val spentBoxIdsByTokenIdEndpoint: ZServerEndpoint[BoxService, Any] =
    spentBoxIdsByTokenId.zServerLogic { case (tokenId, qp) =>
      BoxService
        .getSpentBoxesByTokenId(tokenId, qp.toMap)
        .map(_.map(_.boxId))
        .mapError(handleThrowable)
    }

  protected[backend] val anyBoxesByTokenId: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[Box], Any] =
    endpoint.get
      .in(rootPath / "boxes" / "any" / "by-token-id" / path[String]("tokenId"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[Box]])

  protected[backend] val anyBoxesByTokenIdEndpoint: ZServerEndpoint[BoxService, Any] =
    anyBoxesByTokenId.zServerLogic { case (tokenId, qp) =>
      BoxService
        .getAnyBoxesByTokenId(tokenId, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val anyBoxIdsByTokenId: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxId], Any] =
    endpoint.get
      .in(rootPath / "box-ids" / "any" / "by-token-id" / path[String]("tokenId"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxId]])

  protected[backend] val anyBoxIdsByTokenIdEndpoint: ZServerEndpoint[BoxService, Any] =
    anyBoxIdsByTokenId.zServerLogic { case (tokenId, qp) =>
      BoxService
        .getAnyBoxesByTokenId(tokenId, qp.toMap)
        .map(_.map(_.boxId))
        .mapError(handleThrowable)
    }
