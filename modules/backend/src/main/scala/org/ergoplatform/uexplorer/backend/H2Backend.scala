package org.ergoplatform.uexplorer.backend

import com.zaxxer.hikari.HikariDataSource
import io.getquill.JdbcContextConfig
import io.getquill.util.LoadConfig
import org.ergoplatform.uexplorer.BlockId
import org.ergoplatform.uexplorer.backend.blocks.{BlockRepo, BlockRoutes, BlockTapirRoutes}
import org.ergoplatform.uexplorer.backend.boxes.{BoxRoutes, BoxService, BoxTapirRoutes}
import org.ergoplatform.uexplorer.db.{Backend, LinkedBlock}
import zio.*
import zio.http.*

import javax.sql.DataSource

object H2Backend extends Backend {

  def layer: ZLayer[Any, Throwable, HikariDataSource] = ZLayer.scoped(
    ZIO.acquireRelease(
      ZIO.attempt(JdbcContextConfig(LoadConfig("h2")).dataSource)
    )(ds => ZIO.log(s"Closing h2 backend") *> ZIO.succeed(ds.close()))
  )

  private val sslConfig = SSLConfig.fromResource(
    behaviour = SSLConfig.HttpBehaviour.Accept,
    certPath  = "server.crt",
    keyPath   = "server.key"
  )

  def install(): ZIO[BoxService with BlockRepo with Server, Nothing, Int] =
    Server
      .install((BlockTapirRoutes.routes ++ BoxTapirRoutes.routes).withDefaultErrorResponse)
      .logError("Serving at 8090 failed.")

  def serve(port: Int): ZIO[DataSource with BoxService with BlockRepo, Throwable, Nothing] =
    (install() *> ZIO.never).provideSomeLayer(
      Server.defaultWith(
        _.port(port)
          .ssl(sslConfig)
      )
    )

  override def isEmpty: Task[Boolean] = ???

  override def removeBlocks(blockIds: Set[BlockId]): Task[Unit] = ???

  override def writeBlock(b: LinkedBlock, condition: Task[Any]): Task[BlockId] = ???

  override def close(): Task[Unit] = ???
}
