package org.ergoplatform.uexplorer.backend

import org.ergoplatform.uexplorer.backend.blocks.{BlockRepo, BlockTapirRoutes}
import org.ergoplatform.uexplorer.backend.boxes.{BoxService, BoxTapirRoutes}
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.swagger.bundle.SwaggerInterpreter
import zio.RIO
import sttp.tapir.ztapir.*
import zio.http.*

object TapirRoutes extends BlockTapirRoutes with BoxTapirRoutes:

  val blockSwaggerEndpoints = List(infoEndpoint, blockByIdEndpoint, blockByIdsEndpoint)

  val boxSwaggerEndpoints =
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
    )

  val blockRoutes: List[ZServerEndpoint[BlockRepo, Any]] =
    List(infoServerEndpoint, blockByIdServerEndpoint, blockByIdsServerEndpoint)

  val boxRoutes: List[ZServerEndpoint[BoxService, Any]] =
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
    )

  val swaggerEndpoints: List[ServerEndpoint[Any, RIO[BoxService with BlockRepo, *]]] =
    SwaggerInterpreter()
      .fromEndpoints[RIO[BoxService with BlockRepo, *]](boxSwaggerEndpoints ++ blockSwaggerEndpoints, "uexplorer api", "1.0")

  val routes: HttpApp[BoxService with BlockRepo, Throwable] =
    ZioHttpInterpreter().toHttp[BoxService](boxRoutes) ++ ZioHttpInterpreter().toHttp[BlockRepo](blockRoutes) ++ ZioHttpInterpreter()
      .toHttp[BoxService with BlockRepo](swaggerEndpoints)
