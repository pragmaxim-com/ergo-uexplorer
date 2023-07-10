package org.ergoplatform.uexplorer.db

import akka.NotUsed
import akka.stream.scaladsl.Flow
import org.ergoplatform.uexplorer.BlockId

import java.util.concurrent
import java.util.concurrent.Flow.*
import scala.concurrent.Future
import scala.util.Try

trait Backend {

  def isEmpty: Future[Boolean]

  def removeBlocks(blockIds: Set[BlockId]): Future[Unit]

  def blockWriteFlow: Flow[BestBlockInserted, BestBlockInserted, NotUsed]

  def writeBlock(b: NormalizedBlock): BlockId

  def close(): Future[Unit]
}
