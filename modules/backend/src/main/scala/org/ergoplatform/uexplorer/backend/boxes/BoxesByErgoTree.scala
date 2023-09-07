package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.backend.{Codecs, ErrorResponse, TapirRoutes}
import org.ergoplatform.uexplorer.db.{Box, Utxo}
import org.ergoplatform.uexplorer.{BlockId, BoxId, TxId}
import sttp.model.{QueryParams, StatusCode}
import sttp.tapir.generic.auto.*
import sttp.tapir.json.zio.*
import sttp.tapir.ztapir.*
import sttp.tapir.{PublicEndpoint, Schema, queryParams}
import zio.*
import zio.json.*

trait BoxesByErgoTree extends TapirRoutes with Codecs:

  protected[backend] val spentContractBoxesByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[Box], Any] =
    endpoint.get
      .in(rootPath / "boxes" / "spent" / "contracts" / "by-ergo-tree" / path[String]("ergoTree"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[Box]])
      .description("Get spent boxes by ErgoTree (base16)")

  protected[backend] val spentContractBoxesByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    spentContractBoxesByErgoTree.zServerLogic { case (ergoTree, qp) =>
      BoxService
        .getSpentBoxesByErgoTree(ergoTree, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val spentContractBoxIdsByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxId], Any] =
    endpoint.get
      .in(rootPath / "box-ids" / "spent" / "contracts" / "by-ergo-tree" / path[String]("ergoTree"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxId]])
      .description("Get spent box IDs by ErgoTree (base16)")

  protected[backend] val spentContractBoxIdsByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    spentContractBoxIdsByErgoTree.zServerLogic { case (ergoTree, qp) =>
      BoxService
        .getSpentBoxesByErgoTree(ergoTree, qp.toMap)
        .map(_.map(_.boxId))
        .mapError(handleThrowable)
    }

  protected[backend] val unspentContractBoxesByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[Utxo], Any] =
    endpoint.get
      .in(rootPath / "boxes" / "unspent" / "contracts" / "by-ergo-tree" / path[String]("ergoTree"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[Utxo]])
      .description("Get unspent boxes by ErgoTree (base16)")

  protected[backend] val unspentContractBoxesByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentContractBoxesByErgoTree.zServerLogic { case (ergoTree, qp) =>
      BoxService
        .getUnspentBoxesByErgoTree(ergoTree, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val unspentContractBoxIdsByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxId], Any] =
    endpoint.get
      .in(rootPath / "box-ids" / "unspent" / "contracts" / "by-ergo-tree" / path[String]("ergoTree"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxId]])
      .description("Get unspent box IDs by ErgoTree (base16)")

  protected[backend] val unspentContractBoxIdsByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentContractBoxIdsByErgoTree.zServerLogic { case (ergoTree, qp) =>
      BoxService
        .getUnspentBoxesByErgoTree(ergoTree, qp.toMap)
        .map(_.map(_.boxId))
        .mapError(handleThrowable)
    }

  protected[backend] val anyContractBoxesByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[Box], Any] =
    endpoint.get
      .in(rootPath / "boxes" / "any" / "contracts" / "by-ergo-tree" / path[String]("ergoTree"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[Box]])
      .description("Get any (spent or unspent) boxes by ErgoTree (base16)")

  protected[backend] val anyContractBoxesByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    anyContractBoxesByErgoTree.zServerLogic { case (ergoTree, qp) =>
      BoxService
        .getAnyBoxesByErgoTree(ergoTree, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val anyContractBoxIdsByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxId], Any] =
    endpoint.get
      .in(rootPath / "box-ids" / "any" / "contracts" / "by-ergo-tree" / path[String]("ergoTree"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxId]])
      .description("Get any (spent or unspent) box IDs by ErgoTree (base16)")

  protected[backend] val anyContractBoxIdsByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    anyContractBoxIdsByErgoTree.zServerLogic { case (ergoTree, qp) =>
      BoxService
        .getAnyBoxesByErgoTree(ergoTree, qp.toMap)
        .map(_.map(_.boxId))
        .mapError(handleThrowable)

    }
