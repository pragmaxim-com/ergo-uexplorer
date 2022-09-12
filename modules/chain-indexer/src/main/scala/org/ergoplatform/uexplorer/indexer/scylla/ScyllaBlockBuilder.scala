package org.ergoplatform.uexplorer.indexer.scylla

import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout
import akka.{Done, NotUsed}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.BlockId
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.explorer.protocol.models.ApiFullBlock
import org.ergoplatform.explorer.settings.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.Const
import org.ergoplatform.uexplorer.indexer.api.BlockBuilder
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor._
import org.ergoplatform.uexplorer.indexer.scylla.entity._

import scala.collection.JavaConverters.seqAsJavaList
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class ScyllaBlockBuilder(implicit protocol: ProtocolSettings, cqlSession: CqlSession, s: ActorSystem[Nothing])
  extends BlockBuilder
  with LazyLogging {

  import ScyllaBlockBuilder._

  implicit private val timeout: Timeout = 3.seconds

  private val updateMainChainPreparedStatements: Map[String, (Option[String], PreparedStatement)] =
    updateMainChainStatements.map { case (table, keyOpt, statement) =>
      table -> (keyOpt, cqlSession.prepare(statement))
    }.toMap

  private def getBestBlockOrBranch(
    block: ApiFullBlock,
    blockHttpClient: BlockHttpClient,
    progressMonitor: ActorRef[ProgressMonitorRequest],
    acc: List[ApiFullBlock]
  ): Future[List[ApiFullBlock]] =
    progressMonitor
      .ask(ref => GetBlock(block.header.parentId, ref))
      .flatMap {
        case CachedBlock(Some(_)) =>
          Future.successful(block :: acc)
        case CachedBlock(None) if block.header.height == 1 =>
          Future.successful(block :: acc)
        case CachedBlock(None) =>
          logger.info(s"Encountered fork at height ${block.header.height} and block ${block.header.id}")
          blockHttpClient
            .getBlockForId(block.header.parentId)
            .flatMap(b => getBestBlockOrBranch(b, blockHttpClient, progressMonitor, block :: acc))
      }

  private def removeBlocksFromMainChain(blockIds: List[BlockId]): Future[Done] =
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

  def blockBuildingFlow(
    blockHttpClient: BlockHttpClient,
    progressMonitor: ActorRef[ProgressMonitorRequest]
  ): Flow[ApiFullBlock, FlatBlock, NotUsed] =
    Flow[ApiFullBlock]
      .mapAsync(1) { block =>
        getBestBlockOrBranch(block, blockHttpClient, progressMonitor, List.empty)
          .flatMap {
            case bestBlock :: Nil =>
              progressMonitor.ask(ref => InsertBestBlock(bestBlock, ref))
            case winningFork =>
              progressMonitor.ask(ref => InsertWinningFork(winningFork, ref))
          }
      }
      .mapAsync(1) {
        case BestBlockInserted(flatBlock) =>
          Future.successful(List(flatBlock))
        case WinningForkInserted(newFlatBlocks, supersededFork) =>
          removeBlocksFromMainChain(supersededFork.map(_.stats.headerId))
            .map(_ => newFlatBlocks)
      }
      .mapConcat(identity)
}

object ScyllaBlockBuilder {

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
