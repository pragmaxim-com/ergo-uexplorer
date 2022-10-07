package org.ergoplatform.uexplorer.indexer.cassandra.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{BoundStatement, DefaultBatchType, PreparedStatement}
import org.ergoplatform.uexplorer.db.FlatBlock
import org.ergoplatform.uexplorer.indexer.cassandra.CassandraBackend

trait CassandraInputsWriter { this: CassandraBackend =>
  import Inputs._

  def inputsWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed] =
    storeBlockBatchFlow(
      parallelism,
      batchType = DefaultBatchType.LOGGED,
      buildInsertStatement(columns, node_inputs_table),
      inputInsertBinder
    )

  protected[cassandra] def inputInsertBinder: (FlatBlock, PreparedStatement) => List[BoundStatement] = {
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
        input.proofBytes match {
          case Some(proofBytes) =>
            partialStatement.setString(proof_bytes, proofBytes.unwrapped)
          case None =>
            partialStatement.setToNull(proof_bytes)
        }
      }
  }

}

object Inputs {
  protected[cassandra] val node_inputs_table = "node_inputs"

  protected[cassandra] val box_id      = "box_id"
  protected[cassandra] val tx_id       = "tx_id"
  protected[cassandra] val header_id   = "header_id"
  protected[cassandra] val proof_bytes = "proof_bytes"
  protected[cassandra] val extension   = "extension"
  protected[cassandra] val idx         = "idx"
  protected[cassandra] val main_chain  = "main_chain"

  protected[cassandra] val columns = Seq(
    box_id,
    tx_id,
    header_id,
    proof_bytes,
    extension,
    idx,
    main_chain
  )
}
