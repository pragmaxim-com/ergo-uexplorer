package org.ergoplatform.uexplorer.cassandra.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{BoundStatement, DefaultBatchType, PreparedStatement}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.cassandra.CassandraBackend
import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.db.{BestBlockInserted, FullBlock}

import scala.collection.immutable.ArraySeq

trait CassandraAssetsWriter extends LazyLogging {
  this: CassandraBackend =>

  import Assets._

  def assetsWriteFlow(parallelism: Int): Flow[BestBlockInserted, BestBlockInserted, NotUsed] =
    storeBatchFlow(
      parallelism,
      batchType = DefaultBatchType.LOGGED,
      buildInsertStatement(columns, node_assets_table),
      assetsInsertBinder
    )

  protected[cassandra] def assetsInsertBinder: (BestBlockInserted, PreparedStatement) => ArraySeq[BoundStatement] = {
    case (BestBlockInserted(_, Some(block)), statement) =>
      block.assets.map { asset =>
        statement
          .bind()
          // format: off
          .setString(header_id,     asset.headerId)
          .setString(token_id,      asset.tokenId)
          .setString(box_id,        asset.boxId.unwrapped)
          .setInt(idx,              asset.index)
          .setLong(value,           asset.amount)
          // format: on
      }
    case _ =>
      throw new IllegalStateException("Backend must be enabled")
  }

}

object Assets {
  protected[cassandra] val node_assets_table = "node_assets"

  protected[cassandra] val header_id = "header_id"
  protected[cassandra] val token_id  = "token_id"
  protected[cassandra] val box_id    = "box_id"
  protected[cassandra] val idx       = "idx"
  protected[cassandra] val value     = "value"

  protected[cassandra] val columns = Seq(
    header_id,
    token_id,
    box_id,
    idx,
    value
  )
}
