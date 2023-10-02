package org.ergoplatform.uexplorer.backend

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.backend.blocks.{BlockRoutesSpec, BlockService, PersistentBlockRepo}
import org.ergoplatform.uexplorer.backend.boxes.{BoxRoutesSpec, BoxService, PersistentAssetRepo, PersistentBoxRepo}
import org.ergoplatform.uexplorer.backend.stats.{StatsRoutesSpec, StatsService}
import org.ergoplatform.uexplorer.http.{NodePool, NodePoolConf, Rest}
import org.ergoplatform.uexplorer.storage.{MvStorage, MvStoreConf}
import org.ergoplatform.uexplorer.{CoreConf, NetworkPrefix}
import zio.ZIO
import zio.http.*
import zio.test.{TestAspect, ZIOSpecDefault}

object RouteSpec extends ZIOSpecDefault with BlockRoutesSpec with BoxRoutesSpec with ProxyRoutesSpec with StatsRoutesSpec {

  implicit private val ps: CoreConf            = CoreConf(NetworkPrefix.fromStringUnsafe("0"))
  implicit private val enc: ErgoAddressEncoder = ps.addressEncoder

  private val routeLayers =
    Client.default >+>
      CoreConf.layer >+>
      MvStoreConf.layer >+>
      NodePoolConf.layer >+>
      NodePool.layerInitializedFromConf >+>
      H2Backend.zlayerWithTempDirPerEachRun >+>
      MvStorage.zlayerWithTempDirPerEachRun >+>
      PersistentBoxRepo.layer >+>
      PersistentAssetRepo.layer >+>
      PersistentBlockRepo.layer >+>
      PersistentRepo.layer >+>
      StatsService.layer >+>
      BoxService.layer >+>
      BlockService.layer

  def spec = (suite("RouteSpec")(
    blockRoutesSpec(tapirWithProxyRoutes),
    blockRoutesSpec(zioHttpWithProxyRoutes),
    boxRoutesSpec(tapirWithProxyRoutes),
    boxRoutesSpec(zioHttpWithProxyRoutes),
    proxyRoutesSpec(tapirWithProxyRoutes),
    proxyRoutesSpec(zioHttpWithProxyRoutes),
    statsRoutesSpec(tapirWithProxyRoutes),
    statsRoutesSpec(zioHttpWithProxyRoutes)
  ) @@ TestAspect.beforeAll(
    for
      repo   <- ZIO.service[Repo]
      blocks <- Rest.Blocks.regular.forHeights(1 to 10)
      _      <- ZIO.collectAllDiscard(blocks.map(b => repo.persistBlock(b)))
    yield ()
  )).provideShared(routeLayers)
}
