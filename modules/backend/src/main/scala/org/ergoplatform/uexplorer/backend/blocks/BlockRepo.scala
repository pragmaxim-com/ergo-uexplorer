package org.ergoplatform.uexplorer.backend.blocks

import org.ergoplatform.uexplorer.BlockId
import zio.*
import org.ergoplatform.uexplorer.db.Block

trait BlockRepo:
  def insert(block: Block): Task[BlockId]

  def lookup(headerId: BlockId): Task[Option[Block]]

  def lookupBlocks(ids: Set[BlockId]): Task[List[Block]]

  def isEmpty: Task[Boolean]

  def delete(blockId: BlockId): Task[Long]

  def delete(blockIds: Iterable[BlockId]): Task[Long]

object BlockRepo:
  def insert(block: Block): ZIO[BlockRepo, Throwable, BlockId] =
    ZIO.serviceWithZIO[BlockRepo](_.insert(block))

  def lookup(headerId: BlockId): ZIO[BlockRepo, Throwable, Option[Block]] =
    ZIO.serviceWithZIO[BlockRepo](_.lookup(headerId))

  def lookupBlocks(ids: Set[BlockId]): ZIO[BlockRepo, Throwable, List[Block]] =
    ZIO.serviceWithZIO[BlockRepo](_.lookupBlocks(ids))

  def isEmpty: ZIO[BlockRepo, Throwable, Boolean] =
    ZIO.serviceWithZIO[BlockRepo](_.isEmpty)

  def delete(blockId: BlockId): ZIO[BlockRepo, Throwable, Long] =
    ZIO.serviceWithZIO[BlockRepo](_.delete(blockId))

  def delete(ids: Iterable[BlockId]): ZIO[BlockRepo, Throwable, Long] =
    ZIO.serviceWithZIO[BlockRepo](_.delete(ids))
