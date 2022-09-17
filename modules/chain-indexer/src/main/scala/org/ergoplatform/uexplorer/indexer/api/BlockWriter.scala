package org.ergoplatform.uexplorer.indexer.api

import akka.NotUsed
import akka.stream.scaladsl.Flow
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor.Inserted

trait BlockWriter {
  def blockInfoWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed]

  def headerWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed]

  def transactionsWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed]

  def assetsWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed]

  def registersWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed]

  def tokensWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed]

  def outputsWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed]

  def inputsWriteFlow(parallelism: Int): Flow[FlatBlock, FlatBlock, NotUsed]

  def blockWriteFlow: Flow[Inserted, FlatBlock, NotUsed]

  def blockUpdaterFlow(parallelism: Int): Flow[Inserted, FlatBlock, NotUsed]

}
