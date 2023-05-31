package org.ergoplatform.uexplorer.cassandra.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{BoundStatement, DefaultBatchType, PreparedStatement}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.cassandra.CassandraBackend
import eu.timepit.refined.auto.*

import scala.collection.immutable.ArraySeq
import org.ergoplatform.uexplorer.db.BestBlockInserted

trait CassandraTokensWriter extends LazyLogging {
  this: CassandraBackend =>

  import Tokens._

  def tokensWriteFlow(parallelism: Int): Flow[BestBlockInserted, BestBlockInserted, NotUsed] =
    storeBatchFlow(
      parallelism,
      batchType = DefaultBatchType.LOGGED,
      buildInsertStatement(columns, node_tokens_table),
      tokensInsertBinder
    )

  protected[cassandra] def tokensInsertBinder: (BestBlockInserted, PreparedStatement) => ArraySeq[BoundStatement] = {
    case (BestBlockInserted(block, _), statement) =>
      block.tokens.map { t =>
        val partialStatement =
          statement
            // format: off
            .bind()
            .setString(header_id,     block.header.id)
            .setString(token_id,      t.id)
            .setString(box_id,        t.boxId.unwrapped)
            .setLong(emission_amount, t.emissionAmount)
            .setInt(decimals,         t.decimals.getOrElse(0))
            // format: on

        val ps1 = t.name.fold(partialStatement)(partialStatement.setString(name, _))
        val ps2 = t.description.fold(ps1)(ps1.setString(description, _))
        t.`type`.map(_.unwrapped).fold(ps2)(ps2.setString(`type`, _))
      }
  }

}

object Tokens {
  protected[cassandra] val node_tokens_table = "node_tokens"

  protected[cassandra] val header_id       = "header_id"
  protected[cassandra] val token_id        = "token_id"
  protected[cassandra] val box_id          = "box_id"
  protected[cassandra] val emission_amount = "emission_amount"
  protected[cassandra] val name            = "name"
  protected[cassandra] val description     = "description"
  protected[cassandra] val `type`          = "type"
  protected[cassandra] val decimals        = "decimals"

  protected[cassandra] val columns = Seq(
    header_id,
    token_id,
    box_id,
    emission_amount,
    name,
    description,
    `type`,
    decimals
  )
}
