package org.ergoplatform.uexplorer.indexer.cassandra.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.uexplorer.indexer.cassandra.CassandraBackend

import java.nio.ByteBuffer

trait CassandraHeaderWriter extends LazyLogging { this: CassandraBackend =>
  import Headers._

  def headerWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed] =
    storeBlockFlow(
      parallelism,
      buildInsertStatement(columns, node_headers_table),
      headerInsertBinder
    )

  protected[cassandra] def headerInsertBinder: (FlatBlock, PreparedStatement) => BoundStatement = { case (block, statement) =>
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
  protected[cassandra] val node_headers_table = "node_headers"

  protected[cassandra] val header_id         = "header_id"
  protected[cassandra] val parent_id         = "parent_id"
  protected[cassandra] val version           = "version"
  protected[cassandra] val height            = "height"
  protected[cassandra] val n_bits            = "n_bits"
  protected[cassandra] val difficulty        = "difficulty"
  protected[cassandra] val timestamp         = "timestamp"
  protected[cassandra] val state_root        = "state_root"
  protected[cassandra] val ad_proofs_root    = "ad_proofs_root"
  protected[cassandra] val ad_proofs_bytes   = "ad_proofs_bytes"
  protected[cassandra] val ad_proofs_digest  = "ad_proofs_digest"
  protected[cassandra] val extensions_digest = "extensions_digest"
  protected[cassandra] val extensions_fields = "extensions_fields"
  protected[cassandra] val transactions_root = "transactions_root"
  protected[cassandra] val extension_hash    = "extension_hash"
  protected[cassandra] val miner_pk          = "miner_pk"
  protected[cassandra] val w                 = "w"
  protected[cassandra] val n                 = "n"
  protected[cassandra] val d                 = "d"
  protected[cassandra] val votes             = "votes"
  protected[cassandra] val main_chain        = "main_chain"

  protected[cassandra] val columns = Seq(
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
