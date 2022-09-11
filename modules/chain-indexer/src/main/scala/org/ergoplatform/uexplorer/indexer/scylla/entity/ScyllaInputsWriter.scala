package org.ergoplatform.uexplorer.indexer.scylla.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{BoundStatement, DefaultBatchType, PreparedStatement}
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.uexplorer.indexer.scylla.ScyllaBlockWriter

trait ScyllaInputsWriter { this: ScyllaBlockWriter =>
  import Inputs._

  def inputsWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed] =
    storeBlockBatchFlow(
      parallelism,
      batchType = DefaultBatchType.LOGGED,
      buildInsertStatement(columns, node_inputs_table),
      inputInsertBinder
    )

  protected[scylla] def inputInsertBinder: (FlatBlock, PreparedStatement) => List[BoundStatement] = {
    case (block, statement) =>
      block.inputs.map { input =>
        val partialStatement =
          statement
            .bind()
            // format: off
            .setString(header_id,     input.headerId.value.unwrapped)
            .setString(box_id,        input.boxId.value)
            .setString(tx_id,         input.txId.value)
            .setString(extension,     input.extension.noSpaces)
            .setInt(idx,              input.index)
            .setBoolean(main_chain,   input.mainChain)
            // format: on
        input.proofBytes.map(_.bytes) match {
          case Some(proofBytes) =>
            partialStatement.setByteBuffer(proof_bytes, java.nio.ByteBuffer.wrap(proofBytes))
          case None =>
            partialStatement.setToNull(proof_bytes)
        }
      }
  }

}

object Inputs {
  protected[scylla] val node_inputs_table = "node_inputs"

  protected[scylla] val box_id      = "box_id"
  protected[scylla] val tx_id       = "tx_id"
  protected[scylla] val header_id   = "header_id"
  protected[scylla] val proof_bytes = "proof_bytes"
  protected[scylla] val extension   = "extension"
  protected[scylla] val idx         = "idx"
  protected[scylla] val main_chain  = "main_chain"

  protected[scylla] val columns = Seq(
    box_id,
    tx_id,
    header_id,
    proof_bytes,
    extension,
    idx,
    main_chain
  )
}
