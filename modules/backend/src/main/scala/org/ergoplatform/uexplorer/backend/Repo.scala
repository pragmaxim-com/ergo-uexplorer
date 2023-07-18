package org.ergoplatform.uexplorer.backend

import org.ergoplatform.uexplorer.{BlockId, BoxId}
import org.ergoplatform.uexplorer.backend.Codecs
import org.ergoplatform.uexplorer.db.*
import zio.*

trait Repo:
  def isEmpty: Task[Boolean]

  def removeBlocks(blockIds: Set[BlockId]): Task[Unit]

  def writeBlock(b: NormalizedBlock, condition: Task[Any]): Task[BlockId]

object Repo:
  def writeBlock(b: NormalizedBlock, condition: Task[Any]): ZIO[Repo, Throwable, BlockId] =
    ZIO.serviceWithZIO[Repo](_.writeBlock(b, condition))
