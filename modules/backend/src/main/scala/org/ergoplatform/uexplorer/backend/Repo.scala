package org.ergoplatform.uexplorer.backend

import org.ergoplatform.uexplorer.{BlockId, BoxId}
import org.ergoplatform.uexplorer.db.*
import zio.*

trait Repo:
  def isEmpty: Task[Boolean]

  def removeBlocks(blockIds: List[BlockId]): Task[Unit]

  def writeBlock(b: LinkedBlock, inputIds: Seq[BoxId]): Task[BlockId]

  def writeBlock(b: LinkedBlock): Task[BlockId]

  def getLastBlock: Task[Option[Block]]

  def persistBlock(b: LinkedBlock): Task[BestBlockInserted]

object Repo:
  def writeBlock(b: LinkedBlock, inputIds: Seq[BoxId]): ZIO[Repo, Throwable, BlockId] =
    ZIO.serviceWithZIO[Repo](_.writeBlock(b, inputIds))
