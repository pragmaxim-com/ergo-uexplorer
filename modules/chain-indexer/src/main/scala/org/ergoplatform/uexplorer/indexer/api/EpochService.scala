package org.ergoplatform.uexplorer.indexer.api

import akka.NotUsed
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.scaladsl.Flow
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.explorer.BlockId
import org.ergoplatform.explorer.indexer.models.FlatBlock
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor._
import org.ergoplatform.uexplorer.indexer.progress.{Epoch, InvalidEpochCandidate}

import scala.collection.immutable.TreeMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait EpochService extends LazyLogging {

  def getBlockInfo(headerId: BlockId): Future[BlockInfo]

  def getLastBlockInfoByEpochIndex: Future[TreeMap[Int, BlockInfo]]

  def persistEpoch(epoch: Epoch): Future[Epoch]

  def updateProgressFromDB(progressMonitorRef: ActorRef[ProgressMonitorRequest])(implicit
    s: ActorSystem[Nothing],
    to: Timeout
  ): Future[ProgressState] =
    getLastBlockInfoByEpochIndex.flatMap { lastBlockInfoByEpochIndex =>
      progressMonitorRef
        .ask(ref => UpdateEpochIndexes(lastBlockInfoByEpochIndex, ref))
    }

  def writeEpochFlow(progressMonitorRef: ActorRef[ProgressMonitorRequest])(implicit
    s: ActorSystem[Nothing],
    to: Timeout
  ): Flow[FlatBlock, Either[Int, Epoch], NotUsed] =
    Flow[FlatBlock]
      .filter(b => Epoch.heightAtFlushPoint(b.header.height))
      .mapAsync(1)(b => progressMonitorRef.ask(ref => BlockPersisted(b, ref)))
      .mapAsync(1) {
        case NewEpochCreated(epoch) =>
          persistEpoch(epoch).map(Right(_))
        case NewEpochFailed(InvalidEpochCandidate(epochIndex, invalidHeightsAsc, error)) =>
          logger.error(s"Epoch $epochIndex is invalid due to $error at heights ${invalidHeightsAsc.mkString(",")}")
          Future.successful(Left(epochIndex))
        case NewEpochExisted(epochIndex) =>
          logger.debug(s"Skipping persistence of epoch $epochIndex as it already existed")
          Future.successful(Left(epochIndex))
      }

}
