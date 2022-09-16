package org.ergoplatform.uexplorer.indexer.scylla.entity

import akka.Done
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import org.ergoplatform.explorer.BlockId
import org.ergoplatform.uexplorer.indexer.Const
import org.ergoplatform.uexplorer.indexer.api.BlockUpdater
import org.ergoplatform.uexplorer.indexer.scylla.entity.ScyllaBlockUpdater._

import scala.collection.JavaConverters.seqAsJavaList
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ScyllaBlockUpdater(implicit cqlSession: CqlSession, s: ActorSystem[Nothing]) extends BlockUpdater {

  private val updateMainChainPreparedStatements: Map[String, (Option[String], PreparedStatement)] =
    updateMainChainStatements.map { case (table, keyOpt, statement) =>
      table -> (keyOpt, cqlSession.prepare(statement))
    }.toMap

  def removeBlocksFromMainChain(blockIds: List[BlockId]): Future[Done] =
    Source(blockIds)
      .mapConcat(blockId => updateMainChainPreparedStatements.map { case (table, (key, _)) => (table, key, blockId) })
      .mapAsync(1) {
        case (table, Some(key), blockId) =>
          Source
            .fromPublisher(
              cqlSession.executeReactive(s"SELECT $key FROM ${Const.ScyllaKeyspace}.$table WHERE header_id = '$blockId';")
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
                .toScala
            }

        case (table, None, blockId) =>
          cqlSession
            .executeAsync(updateMainChainBinder(blockId, mainChain = false)(updateMainChainPreparedStatements(table)._2))
            .toScala
      }
      .run()

}

object ScyllaBlockUpdater {

  private def updateMainChainBase(table: String) =
    QueryBuilder
      .update(Const.ScyllaKeyspace, table)
      .setColumn("main_chain", QueryBuilder.bindMarker("main_chain"))
      .whereColumn("header_id")
      .isEqualTo(QueryBuilder.bindMarker("header_id"))

  protected[scylla] val updateMainChainStatements: List[(String, Option[String], SimpleStatement)] =
    List(
      BlocksInfo.block_info_table          -> None,
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

  protected[scylla] def updateMainChainWithKeysBinder(headerId: BlockId, keys: Seq[String], mainChain: Boolean)(
    preparedStatement: PreparedStatement
  ): BoundStatement =
    preparedStatement
      .bind()
      .setBoolean("main_chain", mainChain)
      .setString("header_id", headerId.value.unwrapped)
      .setList("ids", seqAsJavaList(keys), classOf[String])

  protected[scylla] def updateMainChainBinder(headerId: BlockId, mainChain: Boolean)(
    preparedStatement: PreparedStatement
  ): BoundStatement =
    preparedStatement
      .bind()
      .setBoolean("main_chain", mainChain)
      .setString("header_id", headerId.value.unwrapped)
}
