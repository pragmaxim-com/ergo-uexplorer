package org.ergoplatform.uexplorer.backend

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.{CoreConf, NetworkPrefix}
import org.ergoplatform.uexplorer.backend.blocks.{BlockRoutesSpec, PersistentBlockRepo}
import org.ergoplatform.uexplorer.backend.boxes.{BoxRoutesSpec, PersistentBoxRepo}
import org.ergoplatform.uexplorer.http.Rest
import zio.ZIO
import zio.test.{TestAspect, ZIOSpecDefault}

object RouteSpec extends ZIOSpecDefault with BlockRoutesSpec with BoxRoutesSpec with ProxyRoutesSpec {

  implicit private val ps: CoreConf            = CoreConf(NetworkPrefix.fromStringUnsafe("0"))
  implicit private val enc: ErgoAddressEncoder = ps.addressEncoder

  def spec = suite("RouteSpec")(
    blockRoutesSpec(tapirWithProxyRoutes),
    boxRoutesSpec(tapirWithProxyRoutes),
    proxyRoutesSpec(tapirWithProxyRoutes),
    blockRoutesSpec(zioHttpWithProxyRoutes),
    boxRoutesSpec(zioHttpWithProxyRoutes),
    proxyRoutesSpec(zioHttpWithProxyRoutes)
  ) @@ TestAspect.beforeAll(
    (for
      repo   <- ZIO.service[Repo]
      blocks <- Rest.Blocks.regular.forHeights(1 to 10)
      _      <- ZIO.collectAllDiscard(blocks.map(b => repo.writeBlock(b)))
    yield ()).provide(
      H2Backend.zLayerFromConf,
      PersistentBlockRepo.layer,
      PersistentBoxRepo.layer,
      PersistentRepo.layer
    )
  )
}
