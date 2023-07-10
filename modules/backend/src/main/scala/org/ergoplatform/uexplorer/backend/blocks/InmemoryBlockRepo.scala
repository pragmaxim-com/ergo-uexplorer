package org.ergoplatform.uexplorer.backend.blocks

import org.ergoplatform.uexplorer.BlockId
import zio.*

import scala.collection.mutable
import org.ergoplatform.uexplorer.db.Block

case class InmemoryBlockRepo(map: Ref[Map[BlockId, Block]]) extends BlockRepo:

  override def insert(block: Block): UIO[BlockId] =
    map.update(_ + (block.blockId -> block)).as(block.blockId)

  override def lookup(headerId: BlockId): UIO[Option[Block]] =
    map.get.map(_.get(headerId))

  override def lookupBlocks(ids: Set[BlockId]): UIO[List[Block]] =
    map.get.map(_.values.filter(b => ids.contains(b.blockId)).toList)

  override def isEmpty: Task[Boolean] = map.get.map(_.isEmpty)

  override def delete(blockId: BlockId): Task[Long] = map.update(_ - blockId).as(1)

  override def delete(blockIds: Iterable[BlockId]): Task[Long] = map.update(_ -- blockIds).as(1)

object InmemoryBlockRepo {
  def layer: ZLayer[Any, Nothing, InmemoryBlockRepo] =
    ZLayer.fromZIO(
      Ref.make(Map.empty[BlockId, Block]).map(new InmemoryBlockRepo(_))
    )
}
