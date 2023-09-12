package org.ergoplatform.uexplorer.backend

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.backend.blocks.{BlockService, BlockTapirRoutes}
import org.ergoplatform.uexplorer.backend.boxes.{BoxService, BoxTapirRoutes}
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.swagger.SwaggerUIOptions
import sttp.tapir.swagger.bundle.SwaggerInterpreter
import zio.RIO
import sttp.tapir.ztapir.*
import zio.http.*

trait TapirRoutes:
  def rootPath = "explorer"

object TapirRoutes extends BlockTapirRoutes with BoxTapirRoutes:

  val blockSwaggerEndpoints = List(infoEndpoint, blockByIdEndpoint, blockByIdsEndpoint)

  val boxSwaggerEndpoints =
    List(
      unspentBoxesByTokenId,
      spentBoxesByTokenId,
      anyBoxesByTokenId,
      unspentBoxIdsByTokenId,
      spentBoxIdsByTokenId,
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

  val blockRoutes: List[ZServerEndpoint[BlockService, Any]] =
    List(infoServerEndpoint, blockByIdServerEndpoint, blockByIdsServerEndpoint)

  def boxRoutes(implicit enc: ErgoAddressEncoder): List[ZServerEndpoint[BoxService, Any]] =
    List(
      unspentBoxesByTokenIdEndpoint,
      spentBoxesByTokenIdEndpoint,
      anyBoxesByTokenIdEndpoint,
      unspentBoxIdsByTokenIdEndpoint,
      spentBoxIdsByTokenIdEndpoint,
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

  val swaggerEndpoints: List[ServerEndpoint[Any, RIO[BoxService with BlockService, *]]] =
    SwaggerInterpreter(swaggerUIOptions = SwaggerUIOptions.default.pathPrefix(List(rootPath, "swagger")))
      .fromEndpoints[RIO[BoxService with BlockService, *]](boxSwaggerEndpoints ++ blockSwaggerEndpoints, "uexplorer api", "1.0")

  def routes(implicit enc: ErgoAddressEncoder): HttpApp[BoxService with BlockService, Throwable] =
    ZioHttpInterpreter().toHttp[BoxService](boxRoutes) ++ ZioHttpInterpreter().toHttp[BlockService](blockRoutes) ++ ZioHttpInterpreter()
      .toHttp[BoxService with BlockService](swaggerEndpoints)
