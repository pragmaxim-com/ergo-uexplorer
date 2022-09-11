package org.ergoplatform.uexplorer.indexer.scylla.entity

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{BoundStatement, DefaultBatchType, PreparedStatement}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.uexplorer.indexer.scylla.ScyllaBlockWriter

import java.nio.ByteBuffer

trait ScyllaAssetsWriter extends LazyLogging {
  this: ScyllaBlockWriter =>

  import Assets._

  def assetsWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed] =
    storeBlockBatchFlow(
      parallelism,
      batchType = DefaultBatchType.LOGGED,
      buildInsertStatement(columns, node_assets_table),
      assetsInsertBinder
    )

  protected[scylla] def assetsInsertBinder: (FlatBlock, PreparedStatement) => List[BoundStatement] = {
    case (block, statement) =>
      block.assets.map { asset =>
        statement
          .bind()
          // format: off
          .setString(header_id,     asset.headerId.value.unwrapped)
          .setByteBuffer(token_id,  ByteBuffer.wrap(asset.tokenId.value.bytes))
          .setString(box_id,        asset.boxId.value)
          .setInt(idx,              asset.index)
          .setLong(value,           asset.amount)
          // format: on
      }
  }

}

object Assets {
  protected[scylla] val node_assets_table = "node_assets"

  protected[scylla] val header_id = "header_id"
  protected[scylla] val token_id  = "token_id"
  protected[scylla] val box_id    = "box_id"
  protected[scylla] val idx       = "idx"
  protected[scylla] val value     = "value"

  protected[scylla] val columns = Seq(
    header_id,
    token_id,
    box_id,
    idx,
    value
  )
}
