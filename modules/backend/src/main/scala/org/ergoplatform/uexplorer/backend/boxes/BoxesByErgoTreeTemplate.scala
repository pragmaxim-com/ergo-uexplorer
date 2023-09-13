package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.backend.{Codecs, ErrorResponse, TapirRoutes}
import org.ergoplatform.uexplorer.db.{Box, BoxWithAssets, Utxo}
import org.ergoplatform.uexplorer.{BoxId, TxId}
import sttp.model.{QueryParams, StatusCode}
import sttp.tapir.generic.auto.*
import sttp.tapir.json.zio.*
import sttp.tapir.ztapir.*
import sttp.tapir.{queryParams, PublicEndpoint, Schema}
import zio.*
import zio.json.*

trait BoxesByErgoTreeTemplate extends TapirRoutes with Codecs:

  protected[backend] val spentTemplateBoxesByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxWithAssets], Any] =
    endpoint.get
      .in(rootPath / "boxes" / "spent" / "templates" / "by-ergo-tree" / path[String]("ergoTreeT8"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxWithAssets]])
      .description("Get spent boxes by ErgoTree template (base16)")

  protected[backend] def spentTemplateBoxesByErgoTreeEndpoint(implicit enc: ErgoAddressEncoder): ZServerEndpoint[BoxService, Any] =
    spentTemplateBoxesByErgoTree.zServerLogic { case (ergoTreeT8, qp) =>
      BoxService
        .getSpentBoxesByErgoTreeT8(ergoTreeT8, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val spentTemplateBoxIdsByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxId], Any] =
    endpoint.get
      .in(rootPath / "box-ids" / "spent" / "templates" / "by-ergo-tree" / path[String]("ergoTreeT8"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxId]])
      .description("Get spent box IDs by ErgoTree template (base16)")

  protected[backend] val spentTemplateBoxIdsByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    spentTemplateBoxIdsByErgoTree.zServerLogic { case (templates, qp) =>
      BoxService
        .getSpentBoxIdsByErgoTreeT8(templates, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val unspentTemplateBoxesByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxWithAssets], Any] =
    endpoint.get
      .in(rootPath / "boxes" / "unspent" / "templates" / "by-ergo-tree" / path[String]("ergoTreeT8"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxWithAssets]])
      .description("Get unspent boxes by ErgoTree template (base16)")

  protected[backend] def unspentTemplateBoxesByErgoTreeEndpoint(implicit enc: ErgoAddressEncoder): ZServerEndpoint[BoxService, Any] =
    unspentTemplateBoxesByErgoTree.zServerLogic { case (ergoTreeT8, qp) =>
      BoxService
        .getUnspentBoxesByErgoTreeT8(ergoTreeT8, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val unspentTemplateBoxIdsByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxId], Any] =
    endpoint.get
      .in(rootPath / "box-ids" / "unspent" / "templates" / "by-ergo-tree" / path[String]("ergoTreeT8"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxId]])
      .description("Get spent box IDs by ErgoTree template (base16)")

  protected[backend] val unspentTemplateBoxIdsByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    unspentTemplateBoxIdsByErgoTree.zServerLogic { case (ergoTreeT8, qp) =>
      BoxService
        .getUnspentBoxIdsByErgoTreeT8(ergoTreeT8, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val anyTemplateBoxesByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxWithAssets], Any] =
    endpoint.get
      .in(rootPath / "boxes" / "any" / "templates" / "by-ergo-tree" / path[String]("ergoTreeT8"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxWithAssets]])
      .description("Get any (spent or unspent) boxes by ErgoTree template (base16)")

  protected[backend] def anyTemplateBoxesByErgoTreeEndpoint(implicit enc: ErgoAddressEncoder): ZServerEndpoint[BoxService, Any] =
    anyTemplateBoxesByErgoTree.zServerLogic { case (ergoTreeT8, qp) =>
      BoxService
        .getAnyBoxesByErgoTreeT8(ergoTreeT8, qp.toMap)
        .mapError(handleThrowable)
    }

  protected[backend] val anyTemplateBoxIdsByErgoTree: PublicEndpoint[(String, QueryParams), (ErrorResponse, StatusCode), Iterable[BoxId], Any] =
    endpoint.get
      .in(rootPath / "box-ids" / "any" / "templates" / "by-ergo-tree" / path[String]("ergoTreeT8"))
      .in(queryParams)
      .errorOut(jsonBody[ErrorResponse])
      .errorOut(statusCode)
      .out(jsonBody[Iterable[BoxId]])
      .description("Get any (spent or unspent) box IDs by ErgoTree template (base16)")

  protected[backend] val anyTemplateBoxIdsByErgoTreeEndpoint: ZServerEndpoint[BoxService, Any] =
    anyTemplateBoxIdsByErgoTree.zServerLogic { case (ergoTreeT8, qp) =>
      BoxService
        .getAnyBoxIdsByErgoTreeT8(ergoTreeT8, qp.toMap)
        .mapError(handleThrowable)
    }
