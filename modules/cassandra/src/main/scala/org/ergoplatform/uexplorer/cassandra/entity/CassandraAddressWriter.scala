package org.ergoplatform.uexplorer.cassandra.entity

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
import org.ergoplatform.uexplorer.Epoch.{EpochCommand, IgnoreEpoch, WriteNewEpoch}
import org.ergoplatform.uexplorer.cassandra.{AddressPersistenceSupport, AkkaStreamSupport, CassandraBackend}
import org.ergoplatform.uexplorer.db.{BestBlockInserted, Block}
import org.ergoplatform.uexplorer.{Address, BoxId, BoxesByTx, Const, Epoch, MutableMapPimp, TopAddressMap, TxId}

import scala.collection.immutable.{ArraySeq, TreeMap}
import scala.collection.{immutable, mutable}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*
import scala.util.Random

trait CassandraAddressWriter extends AddressPersistenceSupport with LazyLogging {
  this: CassandraBackend =>
  import CassandraAddressWriter.*

  def addressWriteFlow(addressStats: Address => Option[Address.Stats]): Flow[BestBlockInserted, BestBlockInserted, NotUsed] =
    Flow[BestBlockInserted]
      .grouped(64)
      .via(
        storePartitionedBatchFlow(
          parallelism  = 4,
          maxBatchSize = 100,
          batchType    = DefaultBatchType.LOGGED,
          buildInsertStatement(addressColumns, node_addresses_table),
          addressInsertBinder(addressStats)
        )
      )
      .mapConcat(identity)
}

object CassandraAddressWriter extends AkkaStreamSupport with AddressPersistenceSupport with LazyLogging {

  private def bindStatement(
    addr: Address,
    addressPartitionIdx: Short,
    addressType: Const.AddressPrefix,
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

  private def addressPartitioningFlow(addressStats: Address => Option[Address.Stats], stmt: PreparedStatement) =
    Flow.fromFunction[immutable.Seq[BestBlockInserted], Iterable[Vector[BoundStatement]]] { boxesByTxGroup =>
      val result = mutable.Map.empty[(Address, Short), Vector[BoundStatement]]
      boxesByTxGroup.foreach { case BestBlockInserted(_, boxesByTx) =>
        boxesByTx.foreach { case (tx, (inputs, outputs)) =>
          val boxes =
            if (tx.id == Const.Genesis.Emission.tx || tx.id == Const.Genesis.Foundation.tx)
              inputs ++ outputs
            else
              outputs
          boxes.foreach { case (boxId, addr, v) =>
            val addressIndex = addressStats(addr).map(n => n.boxCount / 10000).getOrElse(0).toShort
            val statement =
              bindStatement(
                addr,
                addressIndex,
                Const.getAddressType(addr).get,
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

  protected[cassandra] def addressInsertBinder(
    addressStats: Address => Option[Address.Stats]
  ): (immutable.Seq[BestBlockInserted], PreparedStatement) => Source[Seq[BoundStatement], NotUsed] = {
    case (txBoxesGroup, stmt) =>
      Source
        .single(txBoxesGroup)
        .via(cpuHeavyBalanceFlow(addressPartitioningFlow(addressStats, stmt)))
        .mapConcat(identity)
  }

}
