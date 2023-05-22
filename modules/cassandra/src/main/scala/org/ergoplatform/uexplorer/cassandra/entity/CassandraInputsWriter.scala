package org.ergoplatform.uexplorer.cassandra.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{BoundStatement, DefaultBatchType, PreparedStatement}
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.cassandra.CassandraBackend
import eu.timepit.refined.auto.*

import scala.collection.immutable.ArraySeq

trait CassandraInputsWriter { this: CassandraBackend =>
  import Inputs._

  def inputsWriteFlow(parallelism: Int): Flow[Block, Block, NotUsed] =
    storeBatchFlow(
      parallelism,
      batchType = DefaultBatchType.LOGGED,
      buildInsertStatement(columns, node_inputs_table),
      inputInsertBinder
    )

  protected[cassandra] def inputInsertBinder: (Block, PreparedStatement) => ArraySeq[BoundStatement] = {
    case (block, statement) =>
      block.inputs.map { input =>
        val partialStatement =
          statement
            .bind()
            // format: off
            .setString(header_id,     input.headerId)
            .setString(box_id,        input.boxId.unwrapped)
            .setString(tx_id,         input.txId.unwrapped)
            .setString(extension,     input.extension.noSpaces)
            .setShort(idx,            input.index)
            .setBoolean(main_chain,   input.mainChain)
            // format: on
        input.proofBytes match {
          case Some(proofBytes) =>
            partialStatement.setString(proof_bytes, proofBytes)
          case None =>
            partialStatement // .setToNull(proof_bytes)  // this would create a cassandra tombstone
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
