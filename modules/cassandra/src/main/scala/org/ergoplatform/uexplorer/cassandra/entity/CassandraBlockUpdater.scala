package org.ergoplatform.uexplorer.cassandra.entity

import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import com.datastax.oss.driver.api.core.cql.*
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.BlockId
import org.ergoplatform.uexplorer.cassandra.CassandraBackend
import org.ergoplatform.uexplorer.cassandra.entity.CassandraBlockUpdater.*
import org.ergoplatform.uexplorer.cassandra
import org.ergoplatform.uexplorer.db.Block

import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait CassandraBlockUpdater extends LazyLogging {
  this: CassandraBackend =>

  private val updateMainChainPreparedStatements: Map[String, (Option[String], PreparedStatement)] =
    updateMainChainStatements.map { case (table, keyOpt, statement) =>
      table -> (keyOpt, cqlSession.prepare(statement))
    }.toMap

  def removeBlocksFromMainChain(blockIds: Iterable[BlockId]): Future[Done] =
    Source(blockIds.toList)
      .mapConcat(blockId => updateMainChainPreparedStatements.map { case (table, (key, _)) => (table, key, blockId) })
      .mapAsync(1) {
        case (table, Some(key), blockId) =>
          Source
            .fromPublisher(
              cqlSession.executeReactive(
                s"SELECT $key FROM ${cassandra.Const.CassandraKeyspace}.$table WHERE header_id = '$blockId';"
              )
            )
            .map(_.getString(key))
            .runWith(Sink.seq)
            .flatMap { keys =>
              cqlSession
                .executeAsync(
                  updateMainChainWithKeysBinder(blockId, keys, mainChain = false)(
                    updateMainChainPreparedStatements(table)._2
                  )
                )
                .asScala
            }

        case (table, None, blockId) =>
          cqlSession
            .executeAsync(updateMainChainBinder(blockId, mainChain = false)(updateMainChainPreparedStatements(table)._2))
            .asScala
      }
      .run()

}

object CassandraBlockUpdater {

  private def updateMainChainBase(table: String) =
    QueryBuilder
      .update(cassandra.Const.CassandraKeyspace, table)
      .setColumn("main_chain", QueryBuilder.bindMarker("main_chain"))
      .whereColumn("header_id")
      .isEqualTo(QueryBuilder.bindMarker("header_id"))

  protected[cassandra] val updateMainChainStatements: List[(String, Option[String], SimpleStatement)] =
    List(
      Headers.node_headers_table           -> None,
      Transactions.node_transactions_table -> Some(Transactions.tx_id),
      Inputs.node_inputs_table             -> Some(Inputs.box_id),
      Outputs.node_outputs_table           -> Some(Outputs.box_id)
    ).map {
      case (table, None) =>
        (table, None, updateMainChainBase(table).build())
      case (table, Some(key)) =>
        val q = updateMainChainBase(table).whereColumn(key).in(QueryBuilder.bindMarker("ids")).build()
        (table, Some(key), q)
    }

  protected[cassandra] def updateMainChainWithKeysBinder(headerId: BlockId, keys: Seq[String], mainChain: Boolean)(
    preparedStatement: PreparedStatement
  ): BoundStatement =
    preparedStatement
      .bind()
      .setBoolean("main_chain", mainChain)
      .setString("header_id", headerId.value)
      .setList("ids", keys.asJava, classOf[String])

  protected[cassandra] def updateMainChainBinder(headerId: BlockId, mainChain: Boolean)(
    preparedStatement: PreparedStatement
  ): BoundStatement =
    preparedStatement
      .bind()
      .setBoolean("main_chain", mainChain)
      .setString("header_id", headerId.value)
}
