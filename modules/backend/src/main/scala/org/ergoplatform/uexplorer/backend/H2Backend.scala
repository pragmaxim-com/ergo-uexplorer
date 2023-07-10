package org.ergoplatform.uexplorer.backend

import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.Flow
import akka.{Done, NotUsed}
import com.zaxxer.hikari.HikariDataSource
import io.getquill.JdbcContextConfig
import io.getquill.jdbczio.Quill
import io.getquill.util.LoadConfig
import org.ergoplatform.uexplorer.BlockId
import org.ergoplatform.uexplorer.backend.blocks.{BlockRepo, BlockRoutes, PersistentBlockRepo}
import org.ergoplatform.uexplorer.backend.boxes.{BoxRepo, BoxRoutes, PersistentBoxRepo}
import org.ergoplatform.uexplorer.db.{Backend, BestBlockInserted, Block, NormalizedBlock}
import org.reactivestreams.FlowAdapters
import zio.http.Server
import zio.{Exit, Scope, Unsafe, ZIO, ZLayer}

import javax.sql.DataSource
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

class H2Backend(ds: DataSource) extends Backend {

  override def isEmpty: Future[Boolean] =
    Unsafe.unsafe { implicit unsafe =>
      zio.Runtime.default.unsafe
        .runToFuture(
          for {
            blockEmpty <- BlockRepo.isEmpty.provide(ZLayer.succeed(ds), PersistentBlockRepo.layer)
            boxEmpty   <- BoxRepo.isEmpty.provide(ZLayer.succeed(ds), PersistentBoxRepo.layer)
          } yield blockEmpty && boxEmpty
        )
    }

  override def removeBlocks(blockIds: Set[BlockId]): Future[Unit] =
    Unsafe.unsafe { implicit unsafe =>
      zio.Runtime.default.unsafe
        .runToFuture(
          for {
            _ <- BlockRepo.delete(blockIds).provide(ZLayer.succeed(ds), PersistentBlockRepo.layer)
          } yield ()
        )
    }

  override def blockWriteFlow: Flow[BestBlockInserted, BestBlockInserted, NotUsed] =
    Flow[BestBlockInserted]
      .map { b =>
        writeBlock(b.normalizedBlock)
        b
      }

  override def writeBlock(b: NormalizedBlock): BlockId = {
    val outputs = b.outputRecords
    val inputs  = b.inputRecords.byErgoTree
    val dsLayer = ZLayer.succeed(ds)
    Unsafe.unsafe { implicit unsafe =>
      zio.Runtime.default.unsafe
        .run(
          Repo
            .persistBlockInTx(b.block, outputs, inputs.flatMap(_._2))
            .provide(dsLayer, PersistentBoxRepo.layer, PersistentBlockRepo.layer, PersistentRepo.layer)
        )
        .getOrThrowFiberFailure()
    }
  }

  override def close(): Future[Unit] = ???
}

object H2Backend {
  def apply(implicit s: ActorSystem[Nothing]): Try[H2Backend] = {
    // datasource is not closable, standard layer as we integrate with non-zio world !
    lazy val ds = JdbcContextConfig(LoadConfig("h2")).dataSource
    CoordinatedShutdown(s).addTask(
      CoordinatedShutdown.PhaseServiceStop,
      "close-h2-store"
    ) { () =>
      Future(ds.close()).map(_ => Done)
    }

    Unsafe.unsafe { implicit unsafe =>
      zio.Runtime.default.unsafe
        .run(
          for {
            _ <- Server
                   .install((BlockRoutes() ++ BoxRoutes()).withDefaultErrorResponse)
                   .provide(
                     Server.defaultWithPort(8080),
                     ZLayer.succeed(ds),
                     PersistentBlockRepo.layer,
                     PersistentBoxRepo.layer
                   )
                   .debug("server")
          } yield new H2Backend(ds)
        )
    }.toTry
  }
}
