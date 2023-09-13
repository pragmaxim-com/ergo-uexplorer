package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.backend.{Codecs, ErrorResponse, TapirRoutes}
import org.ergoplatform.uexplorer.db.{Box, BoxWithAssets, Utxo}
import org.ergoplatform.uexplorer.{BlockId, BoxId, TxId}
import sttp.model.{QueryParams, StatusCode}
import sttp.tapir.generic.auto.*
import sttp.tapir.json.zio.*
import sttp.tapir.ztapir.*
import sttp.tapir.{PublicEndpoint, Schema, queryParams}
import zio.*
import zio.json.*

trait BoxesByErgoTreeHash extends TapirRoutes with Codecs:

  protected[backend] val spentContractBoxesByErgoTreeHash: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxWithAssets], Any] =
    endpoint.get
      .in(rootPath / "boxes" / "spent" / "contracts" / "by-ergo-tree-hash" / path[String]("ergoTreeHash"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxWithAssets]])
      .description("Get spent boxes by ErgoTree hash (base16 of Sha256)")

  protected[backend] def spentContractBoxesByErgoTreeHashEndpoint(implicit enc: ErgoAddressEncoder): ZServerEndpoint[BoxService, Any] =
    spentContractBoxesByErgoTreeHash.zServerLogic { case (ergoTreeHash, qp) =>
      BoxService
        .getSpentBoxesByErgoTreeHash(ergoTreeHash, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val spentContractBoxIdsByErgoTreeHash: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxId], Any] =
    endpoint.get
      .in(rootPath / "box-ids" / "spent" / "contracts" / "by-ergo-tree-hash" / path[String]("ergoTreeHash"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxId]])
      .description("Get spent box IDs by ErgoTree hash (base16 of Sha256)")

  protected[backend] val spentContractBoxIdsByErgoTreeHashEndpoint: ZServerEndpoint[BoxService, Any] =
    spentContractBoxIdsByErgoTreeHash.zServerLogic { case (ergoTreeHash, qp) =>
      BoxService
        .getSpentBoxIdsByErgoTreeHash(ergoTreeHash, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val unspentContractBoxesByErgoTreeHash: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxWithAssets], Any] =
    endpoint.get
      .in(rootPath / "boxes" / "unspent" / "contracts" / "by-ergo-tree-hash" / path[String]("ergoTreeHash"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxWithAssets]])
      .description("Get unspent boxes by ErgoTree hash (base16 of Sha256)")

  protected[backend] def unspentContractBoxesByErgoTreeHashEndpoint(implicit enc: ErgoAddressEncoder): ZServerEndpoint[BoxService, Any] =
    unspentContractBoxesByErgoTreeHash.zServerLogic { case (ergoTreeHash, qp) =>
      BoxService
        .getUnspentBoxesByErgoTreeHash(ergoTreeHash, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val unspentContractBoxIdsByErgoTreeHash: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxId], Any] =
    endpoint.get
      .in(rootPath / "box-ids" / "unspent" / "contracts" / "by-ergo-tree-hash" / path[String]("ergoTreeHash"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxId]])
      .description("Get unspent box IDs by ErgoTree hash (base16 of Sha256)")

  protected[backend] val unspentContractBoxIdsByErgoTreeHashEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentContractBoxIdsByErgoTreeHash.zServerLogic { case (ergoTreeHash, qp) =>
      BoxService
        .getUnspentBoxIdsByErgoTreeHash(ergoTreeHash, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val anyContractBoxesByErgoTreeHash: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxWithAssets], Any] =
    endpoint.get
      .in(rootPath / "boxes" / "any" / "contracts" / "by-ergo-tree-hash" / path[String]("ergoTreeHash"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxWithAssets]])
      .description("Get any (spent or unspent) boxes by ErgoTree hash (base16 of Sha256)")

  protected[backend] def anyContractBoxesByErgoTreeHashEndpoint(implicit enc: ErgoAddressEncoder): ZServerEndpoint[BoxService, Any] =
    anyContractBoxesByErgoTreeHash.zServerLogic { case (ergoTreeHash, qp) =>
      BoxService
        .getAnyBoxesByErgoTreeHash(ergoTreeHash, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val anyContractBoxIdsByErgoTreeHash: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxId], Any] =
    endpoint.get
      .in(rootPath / "box-ids" / "any" / "contracts" / "by-ergo-tree-hash" / path[String]("ergoTreeHash"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxId]])
      .description("Get any (spent or unspent) box IDs by ErgoTree hash (base16 of Sha256)")

  protected[backend] val anyContractBoxIdsByErgoTreeHashEndpoint: ZServerEndpoint[BoxService, Any] =
    anyContractBoxIdsByErgoTreeHash.zServerLogic { case (ergoTreeHash, qp) =>
      BoxService
        .getAnyBoxIdsByErgoTreeHash(ergoTreeHash, qp.toMap)
        .mapError(handleThrowable)

    }
