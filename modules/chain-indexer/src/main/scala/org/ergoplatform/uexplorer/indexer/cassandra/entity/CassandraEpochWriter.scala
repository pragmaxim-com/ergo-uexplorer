package org.ergoplatform.uexplorer.indexer.cassandra.entity

import akka.NotUsed
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.core.cql.{BoundStatement, DefaultBatchType, PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.data.TupleValue
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.{bindMarker, insertInto}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.cassandra.{CassandraBackend, EpochPersistenceSupport}
import org.ergoplatform.uexplorer.indexer.chain.ChainStateHolder.*
import org.ergoplatform.uexplorer.indexer.chain.{Epoch, InvalidEpochCandidate}

import scala.jdk.FutureConverters.*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import eu.timepit.refined.auto.*
import org.apache.commons.codec.digest.MurmurHash2
import org.ergoplatform.uexplorer.{Address, BoxId, Const, TxId}
import org.janusgraph.core.{JanusGraphVertex, VertexLabel}

import scala.collection.immutable.{ArraySeq, TreeMap}
import scala.jdk.CollectionConverters.*
import org.apache.tinkerpop.gremlin.structure.{Graph, T, Vertex}
import org.ergoplatform.uexplorer.indexer.{AkkaStreamSupport, Utils}

import scala.collection.mutable

trait CassandraEpochWriter extends AkkaStreamSupport with LazyLogging {
  this: CassandraBackend =>
  import CassandraEpochWriter._

  def epochsWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed] =
    storeBatchFlow(
      parallelism = 1,
      batchType   = DefaultBatchType.LOGGED,
      buildInsertStatement(List(epoch_index, last_header_id), node_epoch_last_headers_table),
      epochLastHeadersInsertBinder
    )

  private def addOutputVertices[G <: Graph](
    tx: G,
    epochIndex: Int
  ) =
    Flow.fromFunction[(TxId, (ArraySeq[(BoxId, Address, Long)], ArraySeq[(BoxId, Address, Long)])), Unit] {
      case (txId, (inputs, outputs)) =>
        val outputBoxIdsByAddressVertex =
          outputs
            .groupBy(_._2)
            .view
            .mapValues(_.map(t => t._1 -> t._3))
            .collect {
              case (address, valueByBox) if address != Const.FeeContract.address =>
                val outputAddressVertexIt = tx.vertices(Utils.vertexHash(address))
                if (outputAddressVertexIt.hasNext)
                  outputAddressVertexIt.next() -> valueByBox
                else
                  tx.addVertex(T.id, Utils.vertexHash(address)) -> valueByBox
            }
            .toList
        inputs
          .filterNot(t => Const.genesisBoxes.contains(t._1))
          .groupBy(_._2)
          .view
          .mapValues(_.map(t => t._1 -> t._3))
          .collect {
            case (address, valueByBoxId) if address != Const.FeeContract.address =>
              val inputAddressVertexIt = tx.vertices(Utils.vertexHash(address))
              if (!inputAddressVertexIt.hasNext) {
                logger.error(s"inputAddress $address from epoch $epochIndex lacks corresponding vertex")
              }
              inputAddressVertexIt.next() -> valueByBoxId
          }
          .foreach { case (inputAddressV, valueByBoxId) =>
            outputBoxIdsByAddressVertex.foreach { case (outputAddressV, outputValueByBoxId) =>
              val txEdge = inputAddressV.addEdge("tx", outputAddressV)
              txEdge.property("txId", txId)
            /*
              valueByBoxId.foreach(txEdge.property("inputs", _))
              outputValueByBoxId.foreach { case (outputBoxId, value) =>
                txEdge.property("outputs", outputBoxId)
                txEdge.property("values", value)
              }
             */
            }
          }
    }

  def boxesWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed] =
    Flow[(Block, Option[MaybeNewEpoch])]
      .mapAsync(1) {
        case (b, s @ Some(NewEpochDetected(e, boxesByHeight))) =>
          val threadedGraph = janusGraph.tx().createThreadedTx[Graph]()
          Source
            .fromIterator(() => boxesByHeight.iterator.flatMap(_._2))
            .via(cpuHeavyBalanceFlow(addOutputVertices(threadedGraph, e.index)))
            .run()
            .map { _ =>
              threadedGraph.tx().commit()
              logger.info(s"New epoch ${e.index} building finished")
              b -> s
            }
        case x => Future.successful(x)
      }
}

object CassandraEpochWriter extends EpochPersistenceSupport with LazyLogging {

  protected[cassandra] def epochLastHeadersInsertBinder
    : ((Block, Option[MaybeNewEpoch]), PreparedStatement) => ArraySeq[BoundStatement] = {
    case ((_, Some(NewEpochDetected(epoch, _))), stmt) =>
      ArraySeq(
        stmt
          .bind()
          .setInt(epoch_index, epoch.index)
          .setString(last_header_id, epoch.blockIds.last)
      )
    case ((_, Some(NewEpochExisted(epochIndex))), _) =>
      logger.debug(s"Skipping persistence of epoch $epochIndex as it already existed")
      ArraySeq.empty
    case _ =>
      ArraySeq.empty
  }

}
