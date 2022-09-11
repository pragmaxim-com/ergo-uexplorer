package org.ergoplatform.uexplorer.indexer.api

import akka.NotUsed
import akka.actor.typed.ActorRef
import akka.stream.scaladsl.Flow
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.explorer.protocol.models.ApiFullBlock
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor.ProgressMonitorRequest

trait BlockBuilder {

  def blockBuildingFlow(
    blockHttpClient: BlockHttpClient,
    progressMonitor: ActorRef[ProgressMonitorRequest]
  ): Flow[ApiFullBlock, FlatBlock, NotUsed]
}
