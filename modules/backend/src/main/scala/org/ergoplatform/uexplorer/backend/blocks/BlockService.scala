package org.ergoplatform.uexplorer.backend.blocks

import org.ergoplatform.uexplorer.{BlockId, TokenId}
import org.ergoplatform.uexplorer.backend.IdParsingException
import org.ergoplatform.uexplorer.db.Block
import zio.{Task, ZIO, ZLayer}

case class BlockService(blockRepo: BlockRepo):

  def insert(block: Block): Task[BlockId] = blockRepo.insert(block)

  def getLastBlocks(n: Int): Task[List[Block]] = blockRepo.getLastBlocks(n)

  def lookup(blockId: String): Task[Option[Block]] =
    for
      hId   <- ZIO.attempt(BlockId.fromStringUnsafe(blockId)).mapError(ex => IdParsingException(blockId, ex.getMessage))
      block <- blockRepo.lookup(hId)
    yield block

  def lookupBlocks(ids: Set[String]): Task[List[Block]] =
    for
      hIds   <- ZIO.attempt(ids.map(BlockId.fromStringUnsafe)).mapError(ex => IdParsingException(ids.mkString(", "), ex.getMessage))
      blocks <- blockRepo.lookupBlocks(hIds)
    yield blocks

  def delete(blockId: String): Task[Long] =
    for
      hId <- ZIO.attempt(BlockId.fromStringUnsafe(blockId)).mapError(ex => IdParsingException(blockId, ex.getMessage))
      l   <- blockRepo.delete(hId)
    yield l

  def delete(blockIds: Set[String]): Task[Long] =
    for
      hIds   <- ZIO.attempt(blockIds.map(BlockId.fromStringUnsafe)).mapError(ex => IdParsingException(blockIds.mkString(", "), ex.getMessage))
      blocks <- blockRepo.delete(hIds)
    yield blocks

  def isEmpty: Task[Boolean] = blockRepo.isEmpty

object BlockService:

  def layer: ZLayer[BlockRepo, Nothing, BlockService] =
    ZLayer.fromFunction(BlockService.apply _)

  def insert(block: Block): ZIO[BlockService, Throwable, BlockId] =
    ZIO.serviceWithZIO[BlockService](_.insert(block))

  def getLastBlocks(n: Int): ZIO[BlockService, Throwable, List[Block]] =
    ZIO.serviceWithZIO[BlockService](_.getLastBlocks(n))

  def lookup(headerId: String): ZIO[BlockService, Throwable, Option[Block]] =
    ZIO.serviceWithZIO[BlockService](_.lookup(headerId))

  def lookupBlocks(ids: Set[String]): ZIO[BlockService, Throwable, List[Block]] =
    ZIO.serviceWithZIO[BlockService](_.lookupBlocks(ids))

  def isEmpty: ZIO[BlockService, Throwable, Boolean] =
    ZIO.serviceWithZIO[BlockService](_.isEmpty)

  def delete(blockId: String): ZIO[BlockService, Throwable, Long] =
    ZIO.serviceWithZIO[BlockService](_.delete(blockId))

  def delete(ids: Set[String]): ZIO[BlockService, Throwable, Long] =
    ZIO.serviceWithZIO[BlockService](_.delete(ids))
