package org.ergoplatform.uexplorer.backend

import com.zaxxer.hikari.HikariDataSource
import io.getquill.JdbcContextConfig
import io.getquill.jdbczio.Quill
import io.getquill.util.LoadConfig
import org.ergoplatform.uexplorer.BlockId
import org.ergoplatform.uexplorer.backend.blocks.{BlockRepo, BlockRoutes, PersistentBlockRepo}
import org.ergoplatform.uexplorer.backend.boxes.{BoxRepo, BoxRoutes, PersistentBoxRepo}
import org.ergoplatform.uexplorer.db.{Backend, BestBlockInserted, Block, NormalizedBlock}
import zio.http.Server
import zio.{Exit, Promise, Scope, Task, Unsafe, ZIO, ZLayer}
import zio.*
import zio.http.{Request, Response}
import zio.http.*
import zio.http.netty.EventLoopGroups

import javax.sql.DataSource
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object H2Backend extends Backend {

  def server(): ZIO[DataSource with BoxRepo with BlockRepo, Throwable, Fiber.Runtime[Nothing, Nothing]] =
    Server
      .serve((BlockRoutes() ++ BoxRoutes()).withDefaultErrorResponse)
      .fork
      .provideSomeLayer(Server.defaultWithPort(8080))

  override def isEmpty: Task[Boolean] = ???

  override def removeBlocks(blockIds: Set[BlockId]): Task[Unit] = ???

  override def writeBlock(b: NormalizedBlock, condition: Task[Any]): Task[BlockId] = ???

  override def close(): Task[Unit] = ???
}
