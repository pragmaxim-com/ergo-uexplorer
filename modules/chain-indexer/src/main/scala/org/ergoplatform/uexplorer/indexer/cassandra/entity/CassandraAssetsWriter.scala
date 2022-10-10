package org.ergoplatform.uexplorer.indexer.cassandra.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{BoundStatement, DefaultBatchType, PreparedStatement}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.cassandra.CassandraBackend
import eu.timepit.refined.auto._

trait CassandraAssetsWriter extends LazyLogging {
  this: CassandraBackend =>

  import Assets._

  def assetsWriteFlow(parallelism: Int): Flow[Block, Block, NotUsed] =
    storeBlockBatchFlow(
      parallelism,
      batchType = DefaultBatchType.LOGGED,
      buildInsertStatement(columns, node_assets_table),
      assetsInsertBinder
    )

  protected[cassandra] def assetsInsertBinder: (Block, PreparedStatement) => List[BoundStatement] = {
    case (block, statement) =>
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
