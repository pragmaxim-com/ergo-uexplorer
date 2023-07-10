package org.ergoplatform.uexplorer.backend

import akka.{Done, NotUsed}
import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.Flow
import com.zaxxer.hikari.HikariDataSource
import io.getquill.JdbcContextConfig
import io.getquill.jdbczio.Quill
import io.getquill.util.LoadConfig
import org.ergoplatform.uexplorer.BlockId
import org.ergoplatform.uexplorer.backend.blocks.{BlockRepo, BlockRoutes, PersistentBlockRepo}
import org.ergoplatform.uexplorer.backend.boxes.{BoxRepo, BoxRoutes, PersistentBoxRepo}
import org.ergoplatform.uexplorer.db.{Backend, BestBlockInserted, NormalizedBlock}
import org.reactivestreams.FlowAdapters
import zio.{Exit, Scope, Unsafe, ZIO, ZLayer}
import zio.http.Server
import org.ergoplatform.uexplorer.db.Block
import concurrent.ExecutionContext.Implicits.global
import javax.sql.DataSource
import scala.concurrent.Future
import scala.util.Try

class H2Backend(dsLayer: ZLayer[Any, Throwable, HikariDataSource]) extends Backend {

  override def isEmpty: Future[Boolean] =
    Unsafe.unsafe { implicit unsafe =>
      zio.Runtime.default.unsafe
        .runToFuture(
          for {
            blockEmpty <- BlockRepo.isEmpty.provide(dsLayer, PersistentBlockRepo.layer)
            boxEmpty   <- BoxRepo.isEmpty.provide(dsLayer, PersistentBoxRepo.layer)
          } yield blockEmpty && boxEmpty
        )
    }

  override def removeBlocks(blockIds: Set[BlockId]): Future[Unit] =
    Unsafe.unsafe { implicit unsafe =>
      zio.Runtime.default.unsafe
        .runToFuture(
          for {
            _ <- BlockRepo.delete(blockIds).provide(dsLayer, PersistentBlockRepo.layer)
          } yield ()
        )
    }

  override def blockWriteFlow: Flow[BestBlockInserted, BestBlockInserted, NotUsed] =
    Flow[BestBlockInserted]
      .map { b =>
        val outputs = b.blockWithInputs.outputRecords
        val inputs  = b.blockWithInputs.inputRecords.byErgoTree
        Unsafe.unsafe { implicit unsafe =>
          zio.Runtime.default.unsafe
            .run(
              for {
                _ <- BlockRepo
                       .insert(b.blockWithInputs.block)
                       .provide(dsLayer, PersistentBlockRepo.layer)
                _ <- BoxRepo
                       .insertUtxos(outputs.byErgoTree.keys, outputs.byErgoTreeT8.keys, outputs.byErgoTree.values.flatten)
                       .provide(dsLayer, PersistentBoxRepo.layer)
                _ <- BoxRepo.deleteUtxos(inputs.flatMap(_._2)).provide(dsLayer, PersistentBoxRepo.layer)
              } yield b
            )
            .getOrThrowFiberFailure()
        }
      }

  override def writeBlock(b: NormalizedBlock): NormalizedBlock = ???

  override def close(): Future[Unit] = ???
}

object H2Backend {
  def apply(implicit s: ActorSystem[Nothing]): Try[H2Backend] = {
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
          } yield new H2Backend(ZLayer.succeed(ds))
        )
    }.toTry
  }
}
