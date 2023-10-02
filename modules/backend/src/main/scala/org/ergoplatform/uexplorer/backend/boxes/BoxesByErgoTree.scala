package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.backend.{Codecs, ErrorResponse, TapirRoutes}
import org.ergoplatform.uexplorer.db.{Box, BoxWithAssets, Utxo}
import org.ergoplatform.uexplorer.{BlockId, BoxId, TxId}
import sttp.model.{QueryParams, StatusCode}
import sttp.tapir.generic.auto.*
import sttp.tapir.json.zio.*
import sttp.tapir.ztapir.*
import sttp.tapir.{queryParams, PublicEndpoint, Schema}
import zio.*
import zio.json.*

trait BoxesByErgoTree extends TapirRoutes with Codecs:

  protected[backend] val spentContractBoxesByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxWithAssets], Any] =
    endpoint.get
      .in(rootPath / "boxes" / "spent" / "contracts" / "by-ergo-tree" / path[String]("ergoTree"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxWithAssets]])
      .description("Get spent boxes by ErgoTree (base16)")

  protected[backend] def spentContractBoxesByErgoTreeEndpoint(implicit enc: ErgoAddressEncoder): ZServerEndpoint[BoxService, Any] =
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
        .getSpentBoxIdsByErgoTree(ergoTree, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val unspentContractBoxesByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxWithAssets], Any] =
    endpoint.get
      .in(rootPath / "boxes" / "unspent" / "contracts" / "by-ergo-tree" / path[String]("ergoTree"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxWithAssets]])
      .description("Get unspent boxes by ErgoTree (base16)")

  protected[backend] def unspentContractBoxesByErgoTreeEndpoint(implicit enc: ErgoAddressEncoder): ZServerEndpoint[BoxService, Any] =
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
        .getUnspentBoxIdsByErgoTree(ergoTree, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val anyContractBoxesByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxWithAssets], Any] =
    endpoint.get
      .in(rootPath / "boxes" / "any" / "contracts" / "by-ergo-tree" / path[String]("ergoTree"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxWithAssets]])
      .description("Get any (spent or unspent) boxes by ErgoTree (base16)")

  protected[backend] def anyContractBoxesByErgoTreeEndpoint(implicit enc: ErgoAddressEncoder): ZServerEndpoint[BoxService, Any] =
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
        .getAnyBoxIdsByErgoTree(ergoTree, qp.toMap)
        .mapError(handleThrowable)

    }
