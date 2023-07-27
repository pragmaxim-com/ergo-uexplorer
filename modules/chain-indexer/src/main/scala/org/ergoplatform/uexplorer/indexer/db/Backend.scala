package org.ergoplatform.uexplorer.indexer.db

import com.zaxxer.hikari.HikariDataSource
import io.getquill.JdbcContextConfig
import io.getquill.util.LoadConfig
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.backend.H2Backend
import org.ergoplatform.uexplorer.backend.blocks.PersistentBlockRepo
import org.ergoplatform.uexplorer.backend.boxes.{BoxService, PersistentBoxRepo}
import org.ergoplatform.uexplorer.db.Backend
import org.ergoplatform.uexplorer.indexer.config.{Cassandra, ChainIndexerConf, H2}
import pureconfig.ConfigReader
import zio.*

import java.util.concurrent.ConcurrentHashMap
import scala.collection.compat.immutable.ArraySeq
import scala.jdk.CollectionConverters.*
import scala.util.Try

object Backend {

  def layerH2: ZLayer[Any, Throwable, HikariDataSource] = ZLayer.scoped(
    ZIO.acquireRelease(
      ZIO.attempt(JdbcContextConfig(LoadConfig("h2")).dataSource)
    )(ds => ZIO.succeed(ds.close()))
  )

  def runServer: ZIO[ChainIndexerConf, Throwable, Fiber.Runtime[Nothing, Nothing]] =
    ZIO.serviceWithZIO[ChainIndexerConf] { conf =>
      conf.backendType match {
        case Cassandra(parallelism) =>
          // CassandraBackend(parallelism) // TODO cassandra must become Repos !
          H2Backend.server().provide(layerH2, CoreConf.layer, BoxService.layer, PersistentBlockRepo.layer, PersistentBoxRepo.layer)
        case H2(parallelism) =>
          H2Backend.server().provide(layerH2, CoreConf.layer, BoxService.layer, PersistentBlockRepo.layer, PersistentBoxRepo.layer)
      }

    }

}
