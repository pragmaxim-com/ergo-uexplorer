package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.{Address, BlockId, BoxId, TxId}
import org.ergoplatform.uexplorer.BoxId.unwrapped
import org.ergoplatform.uexplorer.backend.Codecs
import zio.*
import zio.json.*
import org.ergoplatform.uexplorer.db.{Asset2Box, Block, Box, Utxo}
import zio.{ExitCode, RIO, Task, URIO, ZIO, ZIOAppDefault, ZLayer}
import sttp.tapir.{queryParams, PublicEndpoint, Schema}
import sttp.tapir.generic.auto.*
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.swagger.bundle.SwaggerInterpreter
import sttp.tapir.ztapir.*
import sttp.tapir.json.zio.*
import zio.http.HttpApp
import zio.http.Server
import org.ergoplatform.uexplorer.BlockId.unwrapped
import sttp.model.QueryParams
import sttp.tapir.generic.auto.*
import sttp.tapir.server.ServerEndpoint
import zio.json.{JsonDecoder, JsonEncoder}

object BoxTapirRoutes
  extends BoxesByTokenIdRoutes
  with BoxesByIdRoutes
  with BoxesByAddress
  with BoxesByErgoTree
  with BoxesByErgoTreeHash
  with BoxesByErgoTreeTemplate
  with BoxesByErgoTreeTemplateHash:

  val swaggerEndpoints: Seq[ServerEndpoint[Any, RIO[BoxService, *]]] =
    SwaggerInterpreter()
      .fromEndpoints[RIO[BoxService, *]](
        List(
          unspentAssetsByTokenId,
          spentAssetsByTokenId,
          anyAssetsByTokenId,
          unspentBoxesByTokenId,
          unspentBoxIdsByTokenId,
          spentBoxesByTokenId,
          spentBoxIdsByTokenId,
          anyBoxesByTokenId,
          anyBoxIdsByTokenId,
          unspentBoxById,
          unspentBoxesByIds,
          spentBoxById,
          spentBoxesByIds,
          anyBoxById,
          anyBoxesByIds,
          spentBoxesByAddress,
          spentBoxIdsByAddress,
          unspentBoxesByAddress,
          unspentBoxIdsByAddress,
          anyBoxesByAddress,
          anyBoxIdsByAddress,
          spentContractBoxesByErgoTree,
          spentContractBoxIdsByErgoTree,
          unspentContractBoxesByErgoTree,
          unspentContractBoxIdsByErgoTree,
          anyContractBoxesByErgoTree,
          anyContractBoxIdsByErgoTree,
          spentContractBoxesByErgoTreeHash,
          spentContractBoxIdsByErgoTreeHash,
          unspentContractBoxesByErgoTreeHash,
          unspentContractBoxIdsByErgoTreeHash,
          anyContractBoxesByErgoTreeHash,
          anyContractBoxIdsByErgoTreeHash,
          spentTemplateBoxesByErgoTree,
          spentTemplateBoxIdsByErgoTree,
          unspentTemplateBoxesByErgoTree,
          unspentTemplateBoxIdsByErgoTree,
          anyTemplateBoxesByErgoTree,
          anyTemplateBoxIdsByErgoTree,
          spentTemplateBoxesByErgoTreeHash,
          spentTemplateBoxIdsByErgoTreeHash,
          unspentTemplateBoxesByErgoTreeHash,
          unspentTemplateBoxIdsByErgoTreeHash,
          anyTemplateBoxesByErgoTreeHash,
          anyTemplateBoxIdsByErgoTreeHash
        ),
        "Box api",
        "1.0"
      )

  val routes: HttpApp[BoxService, Throwable] =
    ZioHttpInterpreter()
      .toHttp[BoxService](
        List(
          unspentAssetsByTokenIdEndpoint,
          spentAssetsByTokenIdEndpoint,
          anyAssetsByTokenIdEndpoint,
          unspentBoxesByTokenIdEndpoint,
          unspentBoxIdsByTokenIdEndpoint,
          spentBoxesByTokenIdEndpoint,
          spentBoxIdsByTokenIdEndpoint,
          anyBoxesByTokenIdEndpoint,
          anyBoxIdsByTokenIdEndpoint,
          unspentBoxByIdEndpoint,
          unspentBoxesByIdEndpoint,
          spentBoxByIdEndpoint,
          spentBoxesByIdEndpoint,
          anyBoxByIdEndpoint,
          anyBoxesByIdEndpoint,
          spentBoxesByAddressEndpoint,
          spentBoxIdsByAddressEndpoint,
          unspentBoxesByAddressEndpoint,
          unspentBoxIdsByAddressEndpoint,
          anyBoxesByAddressEndpoint,
          anyBoxIdsByAddressEndpoint,
          spentContractBoxesByErgoTreeEndpoint,
          spentContractBoxIdsByErgoTreeEndpoint,
          unspentContractBoxesByErgoTreeEndpoint,
          unspentContractBoxIdsByErgoTreeEndpoint,
          anyContractBoxesByErgoTreeEndpoint,
          anyContractBoxIdsByErgoTreeEndpoint,
          spentContractBoxesByErgoTreeHashEndpoint,
          spentContractBoxIdsByErgoTreeHashEndpoint,
          unspentContractBoxesByErgoTreeHashEndpoint,
          unspentContractBoxIdsByErgoTreeHashEndpoint,
          anyContractBoxesByErgoTreeHashEndpoint,
          anyContractBoxIdsByErgoTreeHashEndpoint,
          spentTemplateBoxesByErgoTreeEndpoint,
          spentTemplateBoxIdsByErgoTreeEndpoint,
          unspentTemplateBoxesByErgoTreeEndpoint,
          unspentTemplateBoxIdsByErgoTreeEndpoint,
          anyTemplateBoxesByErgoTreeEndpoint,
          anyTemplateBoxIdsByErgoTreeEndpoint,
          spentTemplateBoxesByErgoTreeHashEndpoint,
          spentTemplateBoxIdsByErgoTreeHashEndpoint,
          unspentTemplateBoxesByErgoTreeHashEndpoint,
          unspentTemplateBoxIdsByErgoTreeHashEndpoint,
          anyTemplateBoxesByErgoTreeHashEndpoint,
          anyTemplateBoxIdsByErgoTreeHashEndpoint
        ) ++ swaggerEndpoints
      )
