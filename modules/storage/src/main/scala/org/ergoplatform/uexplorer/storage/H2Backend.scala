package org.ergoplatform.uexplorer.storage

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.Flow
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.BlockId
import org.ergoplatform.uexplorer.db.{Backend, BestBlockInserted, BlockWithInputs, OutputRecord}
import org.ergoplatform.uexplorer.storage.H2Backend.*
import org.h2.jdbcx.JdbcConnectionPool

import java.sql.{Connection, PreparedStatement}
import concurrent.ExecutionContext.Implicits.global
import java.util.concurrent
import org.reactivestreams.FlowAdapters

import java.io.BufferedInputStream
import scala.concurrent.Future
import scala.io.Source
import scala.util.Try

class H2Backend(cp: JdbcConnectionPool)(implicit as: ActorSystem[Nothing]) extends Backend {
  override def isEmpty: Future[Boolean] = Future {
    val conn = cp.getConnection
    try !conn.createStatement().executeQuery("SELECT blockId FROM OUTPUTS limit 1;").next()
    finally conn.close()
  }

  override def removeBlocks(blockIds: Set[BlockId]): Future[Unit] = Future {
    val conn = cp.getConnection
    val ps   = conn.prepareStatement(deleteBlockPreparedStmnt)
    ps.setArray(1, conn.createArrayOf("VARCHAR", blockIds.toArray))
    try !ps.execute()
    finally conn.close()
  }

  override def blockWriteFlow: concurrent.Flow.Processor[BestBlockInserted, BestBlockInserted] =
    FlowAdapters.toFlowProcessor(
      Flow[BestBlockInserted]
        .statefulMap[Connection, BestBlockInserted](() => cp.getConnection)(
          (conn, b) => {
            addOutputInsertBatch(conn.prepareStatement(outputInsertPreparedStmnt), b.blockWithInputs).executeBatch()
            conn.commit()
            conn -> b
          },
          conn => {
            conn.close()
            None
          }
        )
        .toProcessor
        .run()
    )

  override def close(): Future[Unit] = ???
}

object H2Backend extends LazyLogging {
  import org.ergoplatform.uexplorer.HexString.unwrapped

  private def addOutputInsertBatch(ps: PreparedStatement, b: BlockWithInputs) = {
    b.outputRecords.foreach { case OutputRecord(txId, boxId, creationHeight, ergoTreeHex, ergoTreeT8Hex, value, _) =>
      ps.setString(1, boxId.unwrapped)
      ps.setString(2, b.b.header.id.unwrapped)
      ps.setInt(3, creationHeight)
      ps.setString(4, txId.unwrapped)
      ps.setString(5, ergoTreeHex.unwrapped)
      ps.setString(6, ergoTreeT8Hex.map(_.unwrapped).orNull)
      ps.setLong(7, value)
      ps.addBatch()
    }
    ps
  }

  private val deleteBlockPreparedStmnt =
    """DELETE FROM OUTPUTS WHERE blockId IN (?)""".stripMargin

  private val outputInsertPreparedStmnt =
    """INSERT INTO OUTPUTS (boxId, blockId, creationHeight, txId, ergoTreeHex, ergoTreeT8Hex, val) VALUES(?,?,?,?,?,?,?)""".stripMargin

  private val createOutputsTableStmnt =
    Source
      .fromInputStream(
        new BufferedInputStream(Thread.currentThread().getContextClassLoader.getResourceAsStream("db-schema.sql"))
      )
      .getLines()
      .mkString("", "\n", "\n")
      .stripMargin

  def apply()(implicit system: ActorSystem[Nothing]): Try[H2Backend] = Try {
    val h2Conf = system.settings.config.getConfig("h2")
    val cp =
      JdbcConnectionPool.create(
        h2Conf.getString("url"),
        h2Conf.getString("user"),
        h2Conf.getString("password")
      )

    cp.getConnection
      .createStatement()
      .execute(createOutputsTableStmnt)

    CoordinatedShutdown(system).addTask(
      CoordinatedShutdown.PhaseServiceStop,
      "stop-slick-session"
    ) { () =>
      Future(cp.dispose()).map { _ =>
        Done
      }
    }
    new H2Backend(cp)
  }
}
