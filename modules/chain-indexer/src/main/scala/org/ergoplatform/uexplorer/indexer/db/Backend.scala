package org.ergoplatform.uexplorer.indexer.db

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.backend.H2Backend
import org.ergoplatform.uexplorer.backend.blocks.{BlockRepo, BlockService}
import org.ergoplatform.uexplorer.backend.boxes.BoxService
import org.ergoplatform.uexplorer.http.NodePool
import org.ergoplatform.uexplorer.indexer.config.{Cassandra, ChainIndexerConf, H2}
import zio.*
import zio.http.Client

import javax.sql.DataSource

object Backend {

  def runServer: ZIO[Client with NodePool with DataSource with BoxService with BlockService with ChainIndexerConf, Throwable, Nothing] =
    ZIO.serviceWithZIO[ChainIndexerConf] { conf =>
      implicit val enc: ErgoAddressEncoder = conf.core.addressEncoder
      conf.backendType match {
        case Cassandra(parallelism) =>
          // CassandraBackend(parallelism) // TODO cassandra must become Repos !
          H2Backend.serve(8090) // TODO port configurable
        case H2(parallelism) =>
          H2Backend.serve(8090)
      }

    }

}
