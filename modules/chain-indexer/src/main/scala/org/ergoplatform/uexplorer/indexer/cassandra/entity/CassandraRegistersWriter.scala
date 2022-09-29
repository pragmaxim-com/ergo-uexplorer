package org.ergoplatform.uexplorer.indexer.cassandra.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{BoundStatement, DefaultBatchType, PreparedStatement}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.uexplorer.indexer.cassandra.CassandraBackend

trait CassandraRegistersWriter extends LazyLogging {
  this: CassandraBackend =>

  import Registers._

  def registersWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed] =
    storeBlockBatchFlow(
      parallelism,
      batchType = DefaultBatchType.LOGGED,
      buildInsertStatement(columns, node_registers_table),
      registersInsertBinder
    )

  protected[cassandra] def registersInsertBinder: (FlatBlock, PreparedStatement) => List[BoundStatement] = {
    case (block, statement) =>
      block.registers.map { r =>
        // format: off
        statement
          .bind()
          .setString(header_id,             block.header.id.value.unwrapped)
          .setString(id,                    r.id.entryName)
          .setString(box_id,                r.boxId.value)
          .setString(value_type,            r.sigmaType.toString)
          .setString(serialized_value,      r.renderedValue)
        // format: on
      }
  }

}

object Registers {
  protected[cassandra] val node_registers_table = "node_registers"

  protected[cassandra] val header_id        = "header_id"
  protected[cassandra] val id               = "id"
  protected[cassandra] val box_id           = "box_id"
  protected[cassandra] val value_type       = "value_type"
  protected[cassandra] val serialized_value = "serialized_value"
  protected[cassandra] val rendered_value   = "rendered_value"

  protected[cassandra] val columns = Seq(
    header_id,
    id,
    box_id,
    value_type,
    serialized_value,
    rendered_value
  )
}
