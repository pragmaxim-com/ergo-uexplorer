package org.ergoplatform.uexplorer.indexer.scylla.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.uexplorer.indexer.scylla.ScyllaBlockWriter

import java.nio.ByteBuffer

trait ScyllaHeaderWriter extends LazyLogging { this: ScyllaBlockWriter =>
  import Headers._

  def headerWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed] =
    storeBlockFlow(
      parallelism,
      buildInsertStatement(columns, node_headers_table),
      headerInsertBinder
    )

  protected[scylla] def headerInsertBinder: (FlatBlock, PreparedStatement) => BoundStatement = { case (block, statement) =>
    val validVersion =
      if (block.header.version.toInt > 255 || block.header.version.toInt < 0) {
        logger.error(s"Version of block ${block.header.id} is out of [8-bit unsigned] range : ${block.header.version}")
        0: Byte
      } else {
        block.header.version
      }

    val partialStatement =
      // format: off
      statement
        .bind()
        .setString(header_id,             block.header.id.value.unwrapped)
        .setString(parent_id,             block.header.parentId.value.unwrapped)
        .setByte(version,                 validVersion)
        .setInt(height,                   block.header.height)
        .setLong(n_bits,                  block.header.nBits)
        .setBigDecimal(difficulty,        block.header.difficulty.bigDecimal)
        .setLong(timestamp,               block.header.timestamp)
        .setByteBuffer(state_root,        ByteBuffer.wrap(block.header.stateRoot.bytes))
        .setByteBuffer(ad_proofs_root,    ByteBuffer.wrap(block.header.adProofsRoot.bytes))
        .setByteBuffer(extensions_digest, ByteBuffer.wrap(block.extension.digest.bytes))
        .setString(extensions_fields,     block.extension.fields.noSpaces)
        .setByteBuffer(transactions_root, ByteBuffer.wrap(block.header.transactionsRoot.bytes))
        .setByteBuffer(extension_hash,    ByteBuffer.wrap(block.header.extensionHash.bytes))
        .setByteBuffer(miner_pk,          ByteBuffer.wrap(block.header.minerPk.bytes))
        .setByteBuffer(w,                 ByteBuffer.wrap(block.header.w.bytes))
        .setByteBuffer(n,                 ByteBuffer.wrap(block.header.n.bytes))
        .setString(d,                     block.header.d)
        .setString(votes,                 block.header.votes)
        .setBoolean(main_chain,           block.header.mainChain)
        // format: on

    block.adProofOpt.fold(partialStatement) { adProof =>
      partialStatement
        .setByteBuffer(ad_proofs_bytes, ByteBuffer.wrap(adProof.proofBytes.bytes))
        .setByteBuffer(ad_proofs_digest, ByteBuffer.wrap(adProof.digest.bytes))
    }
  }

}

object Headers {
  protected[scylla] val node_headers_table = "node_headers"

  protected[scylla] val header_id         = "header_id"
  protected[scylla] val parent_id         = "parent_id"
  protected[scylla] val version           = "version"
  protected[scylla] val height            = "height"
  protected[scylla] val n_bits            = "n_bits"
  protected[scylla] val difficulty        = "difficulty"
  protected[scylla] val timestamp         = "timestamp"
  protected[scylla] val state_root        = "state_root"
  protected[scylla] val ad_proofs_root    = "ad_proofs_root"
  protected[scylla] val ad_proofs_bytes   = "ad_proofs_bytes"
  protected[scylla] val ad_proofs_digest  = "ad_proofs_digest"
  protected[scylla] val extensions_digest = "extensions_digest"
  protected[scylla] val extensions_fields = "extensions_fields"
  protected[scylla] val transactions_root = "transactions_root"
  protected[scylla] val extension_hash    = "extension_hash"
  protected[scylla] val miner_pk          = "miner_pk"
  protected[scylla] val w                 = "w"
  protected[scylla] val n                 = "n"
  protected[scylla] val d                 = "d"
  protected[scylla] val votes             = "votes"
  protected[scylla] val main_chain        = "main_chain"

  protected[scylla] val columns = Seq(
    header_id,
    parent_id,
    version,
    height,
    n_bits,
    difficulty,
    timestamp,
    state_root,
    ad_proofs_root,
    ad_proofs_bytes,
    ad_proofs_digest,
    extensions_digest,
    extensions_fields,
    transactions_root,
    extension_hash,
    miner_pk,
    w,
    n,
    d,
    votes,
    main_chain
  )
}
