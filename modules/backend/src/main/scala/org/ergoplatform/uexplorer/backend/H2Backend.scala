package org.ergoplatform.uexplorer.backend

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.getquill.JdbcContextConfig
import io.getquill.util.LoadConfig
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.BlockId
import org.ergoplatform.uexplorer.backend.blocks.BlockService
import org.ergoplatform.uexplorer.backend.boxes.BoxService
import org.ergoplatform.uexplorer.db.{Backend, LinkedBlock}
import org.ergoplatform.uexplorer.http.NodePool
import zio.*
import zio.http.*
import org.ergoplatform.uexplorer.{randomNumberPerJvmRun, randomNumberPerRun}

import java.util.Properties
import javax.sql.DataSource
import scala.jdk.CollectionConverters.*

object H2Backend extends Backend {

  private def datasourceProps(newDbName: String): Properties = {
    val properties = new Properties()
    properties.putAll(
      Map(
        "dataSourceClassName" -> "org.h2.jdbcx.JdbcDataSource",
        "dataSource.url" -> s"jdbc:h2:mem:$newDbName;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;INIT=RUNSCRIPT FROM 'classpath:h2-schema.sql'",
        "dataSource.user" -> "user"
      ).asJava
    )
    properties
  }

  private def layer(ds: => HikariDataSource): ZLayer[Any, Throwable, HikariDataSource] = ZLayer.scoped(
    ZIO.acquireRelease(
      ZIO.attempt(ds)
    )(ds => ZIO.log(s"Closing h2 backend") *> ZIO.succeed(ds.close()))
  )

  def zLayerFromConf: ZLayer[Any, Throwable, HikariDataSource] = layer(JdbcContextConfig(LoadConfig("h2")).dataSource)

  def zlayerWithTempDirPerEachRun: ZLayer[Any, Throwable, HikariDataSource] =
    layer(new HikariDataSource(new HikariConfig(datasourceProps(s"test-db-$randomNumberPerRun"))))

  def zlayerWithTempDirPerJvmRun: ZLayer[Any, Throwable, HikariDataSource] =
    layer(new HikariDataSource(new HikariConfig(datasourceProps(s"test-db-$randomNumberPerJvmRun"))))

  private val sslConfig = SSLConfig.fromResource(
    behaviour = SSLConfig.HttpBehaviour.Accept,
    certPath  = "server.crt",
    keyPath   = "server.key"
  )

  def install(implicit enc: ErgoAddressEncoder): ZIO[Client with NodePool with BoxService with BlockService with Server, Nothing, Int] =
    Server
      .install((TapirRoutes.routes ++ ProxyZioRoutes()).withDefaultErrorResponse)
      .logError("Serving at 8090 failed.")

  def serve(port: Int)(implicit enc: ErgoAddressEncoder): ZIO[Client with NodePool with DataSource with BoxService with BlockService, Throwable, Nothing] =
    (install(enc) *> ZIO.never).provideSomeLayer(
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
