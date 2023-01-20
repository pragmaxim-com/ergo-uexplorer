package org.ergoplatform.uexplorer.indexer.cassandra.entity

import akka.NotUsed
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.core.cql.{BoundStatement, DefaultBatchType, PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.data.TupleValue
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.{bindMarker, insertInto}
import com.typesafe.scalalogging.LazyLogging
import eu.timepit.refined.auto.*
import org.apache.commons.codec.digest.MurmurHash2
import org.apache.tinkerpop.gremlin.structure.{Graph, T, Vertex}
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.cassandra.{AddressPersistenceSupport, CassandraBackend}
import org.ergoplatform.uexplorer.indexer.chain.ChainStateHolder.*
import org.ergoplatform.uexplorer.indexer.chain.{Epoch, InvalidEpochCandidate}
import org.ergoplatform.uexplorer.{indexer, Address, BoxId, Const, TopAddressMap, TxId}
import org.ergoplatform.uexplorer.indexer.{AkkaStreamSupport, MutableMapPimp, Utils}
import org.janusgraph.core.{JanusGraphVertex, VertexLabel}
import org.ergoplatform.uexplorer.indexer.MutableMapPimp
import org.ergoplatform.uexplorer.indexer.utxo.TopAddresses
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState.BoxesByTx

import scala.collection.immutable.{ArraySeq, TreeMap}
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*
import scala.util.Random

trait CassandraAddressWriter extends AkkaStreamSupport with AddressPersistenceSupport with LazyLogging {
  this: CassandraBackend =>
  import CassandraAddressWriter.*

  def addressWriteFlow: Flow[(Block, Option[MaybeNewEpoch]), (Block, Option[MaybeNewEpoch]), NotUsed] =
    storePartitionedBatchFlow(
      parallelism  = 4,
      maxBatchSize = 20,
      batchType    = DefaultBatchType.LOGGED,
      buildInsertStatement(addressColumns, node_addresses_table),
      addressInsertBinder
    )

}

object CassandraAddressWriter extends AddressPersistenceSupport with AkkaStreamSupport with LazyLogging {

  private def bindStatement(
    addr: Address,
    addressPartitionIdx: Short,
    addressType: indexer.Const.AddressPrefix,
    addressDescription: String,
    ts: Long,
    txIdx: Short,
    txId: String,
    boxId: String,
    v: Long
  )(stmt: PreparedStatement) =
    stmt
      .bind()
      .setString(address, addr)
      .setShort(address_partition_idx, addressPartitionIdx)
      .setByte(address_type, addressType)
      .setString(address_description, addressDescription)
      .setLong(timestamp, ts)
      .setShort(tx_idx, txIdx)
      .setString(tx_id, txId)
      .setString(box_id, boxId)
      .setLong(value, v)

  private def addressPartitioningFlow(topAddresses: TopAddressMap, stmt: PreparedStatement) =
    Flow.fromFunction[Seq[BoxesByTx], Iterable[Vector[BoundStatement]]] { boxesByTxbyHeight =>
      val result = mutable.Map.empty[(Address, Short), Vector[BoundStatement]]
      boxesByTxbyHeight.foreach { boxesByTx =>
        boxesByTx.foreach { case (tx, (inputs, outputs)) =>
          val boxes =
            if (tx.id == Const.Genesis.Emission.tx || tx.id == Const.Genesis.Foundation.tx)
              inputs ++ outputs
            else
              outputs
          boxes.foreach { case (boxId, addr, v) =>
            val addressIndex = topAddresses.get(addr).map(n => n._2 / 10000).getOrElse(0).toShort
            val statement =
              bindStatement(
                addr,
                addressIndex,
                indexer.Const.getAddressType(addr).get,
                "x",
                tx.timestamp,
                tx.index,
                tx.id.unwrapped,
                boxId.unwrapped,
                v
              )(stmt)
            result.adjust(addr -> addressIndex)(_.fold(Vector(statement))(_ :+ statement))
          }
        }
      }
      result.values
    }

  protected[cassandra] def addressInsertBinder
    : ((Block, Option[MaybeNewEpoch]), PreparedStatement) => Source[Seq[BoundStatement], NotUsed] = {
    case ((_, Some(NewEpochDetected(_, txBoxesByHeight, topAddresses))), stmt) =>
      Source
        .fromIterator(() => txBoxesByHeight.valuesIterator.grouped(64))
        .via(cpuHeavyBalanceFlow(addressPartitioningFlow(topAddresses, stmt)))
        .mapConcat(identity)
    case ((_, Some(NewEpochExisted(epochIndex))), _) =>
      logger.debug(s"Skipping persistence of epoch $epochIndex as it already existed")
      Source.empty
    case _ =>
      Source.empty
  }

}
