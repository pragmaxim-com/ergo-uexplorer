package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.BlockId
import java.util.concurrent.Flow._
import scala.concurrent.Future
import scala.util.Try
import java.util.concurrent.Flow.Processor

trait Backend {

  def isEmpty: Future[Boolean]

  def removeBlocks(blockIds: Set[BlockId]): Future[Unit]

  def blockWriteFlow: Processor[BestBlockInserted, BestBlockInserted]

  def writeBlock(b: BlockWithInputs): BlockWithInputs

  def close(): Future[Unit]
}
