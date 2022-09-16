package org.ergoplatform.uexplorer.indexer.api

import akka.Done
import org.ergoplatform.explorer.BlockId

import scala.concurrent.Future

trait BlockUpdater {
  def removeBlocksFromMainChain(blockIds: List[BlockId]): Future[Done]
}
