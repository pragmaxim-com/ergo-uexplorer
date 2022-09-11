package org.ergoplatform.uexplorer.indexer.scylla.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{BoundStatement, DefaultBatchType, PreparedStatement}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.uexplorer.indexer.scylla.ScyllaBlockWriter

import java.nio.ByteBuffer

trait ScyllaRegistersWriter extends LazyLogging {
  this: ScyllaBlockWriter =>

  import Registers._

  def registersWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed] =
    storeBlockBatchFlow(
      parallelism,
      batchType = DefaultBatchType.LOGGED,
      buildInsertStatement(columns, node_registers_table),
      registersInsertBinder
    )

  protected[scylla] def registersInsertBinder: (FlatBlock, PreparedStatement) => List[BoundStatement] = {
    case (block, statement) =>
      block.registers.map { r =>
        // format: off
        statement
          .bind()
          .setString(header_id,             block.header.id.value.unwrapped)
          .setString(id,                    r.id.entryName)
          .setString(box_id,                r.boxId.value)
          .setString(value_type,            r.sigmaType.toString)
          .setByteBuffer(serialized_value,  ByteBuffer.wrap(r.rawValue.bytes))
          .setString(rendered_value,        r.renderedValue)
        // format: on
      }
  }

}

object Registers {
  protected[scylla] val node_registers_table = "node_registers"

  protected[scylla] val header_id        = "header_id"
  protected[scylla] val id               = "id"
  protected[scylla] val box_id           = "box_id"
  protected[scylla] val value_type       = "value_type"
  protected[scylla] val serialized_value = "serialized_value"
  protected[scylla] val rendered_value   = "rendered_value"

  protected[scylla] val columns = Seq(
    header_id,
    id,
    box_id,
    value_type,
    serialized_value,
    rendered_value
  )
}
