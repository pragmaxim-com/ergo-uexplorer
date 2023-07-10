package org.ergoplatform.uexplorer.backend.blocks

import io.getquill.*
import io.getquill.context.ZioJdbc.DataSourceLayer
import io.getquill.jdbczio.Quill
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.BlockId
import org.ergoplatform.uexplorer.backend.Codecs
import zio.*

import java.util.UUID
import javax.sql.DataSource

case class PersistentBlockRepo(ds: DataSource) extends BlockRepo with Codecs:
  val ctx = new H2ZioJdbcContext(Literal)

  import ctx.*

  override def insert(block: Block): Task[BlockId] =
    ctx
      .run {
        quote {
          query[Block].insertValue {
            lift(block)
          }
        }
      }
      .as(block.blockId)
      .provide(ZLayer.succeed(ds))

  override def lookup(headerId: BlockId): Task[Option[Block]] =
    ctx
      .run {
        quote {
          query[Block]
            .filter(p => p.blockId == lift(headerId))
        }
      }
      .provide(ZLayer.succeed(ds))
      .map(_.headOption)

  override def lookupBlocks(ids: Set[BlockId]): Task[List[Block]] =
    ctx
      .run {
        quote {
          query[Block]
            .filter(p => liftQuery(ids).contains(p.blockId))
        }
      }
      .provide(ZLayer.succeed(ds))

  override def isEmpty: Task[Boolean] =
    ctx
      .run {
        quote {
          query[Block].take(1).isEmpty
        }
      }
      .provide(ZLayer.succeed(ds))

  override def delete(blockId: BlockId): Task[Long] =
    ctx
      .run {
        quote {
          query[Block].filter(p => p.blockId == lift(blockId)).delete
        }
      }
      .provide(ZLayer.succeed(ds))

  override def delete(blockIds: Iterable[BlockId]): Task[Long] =
    ctx
      .run {
        quote {
          query[Block].filter(p => liftQuery(blockIds).contains(p.blockId)).delete
        }
      }
      .provide(ZLayer.succeed(ds))

object PersistentBlockRepo:
  def layer: ZLayer[DataSource, Nothing, PersistentBlockRepo] =
    ZLayer.fromFunction(PersistentBlockRepo(_))
