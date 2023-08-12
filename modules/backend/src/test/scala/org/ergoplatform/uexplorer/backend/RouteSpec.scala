package org.ergoplatform.uexplorer.backend

import org.ergoplatform.uexplorer.{CoreConf, NetworkPrefix}
import org.ergoplatform.uexplorer.backend.blocks.{BlockRoutesSpec, PersistentBlockRepo}
import org.ergoplatform.uexplorer.backend.boxes.{BoxRoutesSpec, PersistentBoxRepo}
import org.ergoplatform.uexplorer.http.Rest
import zio.ZIO
import zio.test.{TestAspect, ZIOSpecDefault}

object RouteSpec extends ZIOSpecDefault with BlockRoutesSpec with BoxRoutesSpec {

  implicit private val ps: CoreConf = CoreConf(NetworkPrefix.fromStringUnsafe("0"))

  def spec = suite("RouteSpec")(
    blockRoutesSpec,
    boxRoutesSpec
  ) @@ TestAspect.beforeAll(
    (for
      repo   <- ZIO.service[Repo]
      blocks <- Rest.chain.forHeights(1 to 10)
      _      <- ZIO.collectAllDiscard(blocks.map(b => repo.writeBlock(b)(ZIO.unit, ZIO.unit)))
    yield ()).provide(
      H2Backend.layer,
      PersistentBlockRepo.layer,
      PersistentBoxRepo.layer,
      PersistentRepo.layer
    )
  )
}
