package org.ergoplatform.uexplorer.indexer.scylla.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{BoundStatement, DefaultBatchType, PreparedStatement}
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.uexplorer.indexer.Const
import org.ergoplatform.uexplorer.indexer.scylla.ScyllaBackend

import java.nio.ByteBuffer

trait ScyllaOutputsWriter { this: ScyllaBackend =>
  import Outputs._

  def outputsWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed] =
    storeBlockBatchFlow(
      parallelism,
      batchType = DefaultBatchType.LOGGED,
      buildInsertStatement(columns, node_outputs_table),
      outputInsertBinder
    )

  protected[scylla] def outputInsertBinder: (FlatBlock, PreparedStatement) => List[BoundStatement] = {
    case (block, statement) =>
      block.outputs.map { output =>
        val partialStatement =
          statement
            .bind()
            // format: off
            .setString(header_id,                   output.headerId.value.unwrapped)
            .setString(box_id,                      output.boxId.value)
            .setString(tx_id,                       output.txId.value)
            .setLong(value,                         output.value)
            .setInt(creation_height,                output.creationHeight)
            .setInt(settlement_height,              output.settlementHeight)
            .setInt(idx,                            output.index)
            .setLong(global_index,                  output.globalIndex)
            .setByteBuffer(ergo_tree,               ByteBuffer.wrap(output.ergoTree.bytes))
            .setByteBuffer(ergo_tree_template_hash, ByteBuffer.wrap(output.ergoTreeTemplateHash.value.bytes))
            .setString(additional_registers,        output.additionalRegisters.noSpaces)
            .setLong(timestamp,                     output.timestamp)
            .setBoolean(main_chain,                 output.mainChain)
          // format: on

        if (output.address.unwrapped != Const.FeeContractAddress)
          partialStatement.setString(address, output.address.unwrapped)
        else
          partialStatement.setToNull(address)
      }
  }
}

object Outputs {
  protected[scylla] val node_outputs_table = "node_outputs"

  protected[scylla] val box_id                  = "box_id"
  protected[scylla] val tx_id                   = "tx_id"
  protected[scylla] val header_id               = "header_id"
  protected[scylla] val value                   = "value"
  protected[scylla] val creation_height         = "creation_height"
  protected[scylla] val settlement_height       = "settlement_height"
  protected[scylla] val idx                     = "idx"
  protected[scylla] val global_index            = "global_index"
  protected[scylla] val ergo_tree               = "ergo_tree"
  protected[scylla] val ergo_tree_template_hash = "ergo_tree_template_hash"
  protected[scylla] val address                 = "address"
  protected[scylla] val additional_registers    = "additional_registers"
  protected[scylla] val timestamp               = "timestamp"
  protected[scylla] val main_chain              = "main_chain"

  protected[scylla] val columns = Seq(
    box_id,
    tx_id,
    header_id,
    value,
    creation_height,
    settlement_height,
    idx,
    global_index,
    ergo_tree,
    ergo_tree_template_hash,
    address,
    additional_registers,
    timestamp,
    main_chain
  )
}
