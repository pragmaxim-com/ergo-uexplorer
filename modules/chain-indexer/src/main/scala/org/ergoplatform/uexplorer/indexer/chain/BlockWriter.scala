package org.ergoplatform.uexplorer.indexer.chain

import akka.NotUsed
import akka.stream.{ActorAttributes, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.{Resiliency, UnexpectedStateError}
import org.ergoplatform.uexplorer.cassandra.api.Backend
import org.ergoplatform.uexplorer.db.{BestBlockInserted, ForkInserted, LinkedBlock, UtxoTracker}
import org.ergoplatform.uexplorer.indexer.chain.StreamExecutor.ChainSyncResult
import org.ergoplatform.uexplorer.janusgraph.api.GraphBackend
import org.ergoplatform.uexplorer.storage.{MvStorage, MvStoreConf}

import concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class BlockWriter(
  storage: MvStorage,
  storageService: StorageService,
  mvStoreConf: MvStoreConf,
  backendOpt: Option[Backend],
  graphBackendOpt: Option[GraphBackend]
)(implicit enc: ErgoAddressEncoder)
  extends LazyLogging {

  private def hasParentAndIsChained(fork: List[LinkedBlock]): Boolean =
    fork.size > 1 && storage.containsBlock(fork.head.info.parentId, fork.head.info.height - 1) &&
      fork.sliding(2).forall {
        case first :: second :: Nil =>
          first.b.header.id == second.info.parentId
        case _ =>
          false
      }

  private def rollbackFork(winningFork: List[LinkedBlock]): Try[ForkInserted] =
    if (!hasParentAndIsChained(winningFork)) {
      Failure(
        new UnexpectedStateError(
          s"Inserting fork ${winningFork.map(_.b.header.id).mkString(",")} at height ${winningFork.map(_.info.height).mkString(",")} illegal"
        )
      )
    } else {
      logger.info(s"Adding fork from height ${winningFork.head.info.height} until ${winningFork.last.info.height}")
      for {
        preForkVersion <- Try(storage.getBlockById(winningFork.head.b.header.id).map(_.revision).get)
        loosingFork = winningFork.flatMap(b => storage.getBlocksByHeight(b.info.height).filter(_._1 != b.b.header.id)).toMap
        _ <- Try(storage.store.rollbackTo(preForkVersion))
      } yield ForkInserted(winningFork, loosingFork)
    }

  private val backendPersistence =
    Flow[BestBlockInserted]
      .buffer(100, OverflowStrategy.backpressure)
      .async
      .via(backendOpt.fold(Flow.fromFunction[BestBlockInserted, BestBlockInserted](identity))(_.blockWriteFlow))

  private val graphPersistenceFlow =
    Flow[BestBlockInserted]
      .buffer(100, OverflowStrategy.backpressure)
      .async
      .via(
        graphBackendOpt.fold(Flow.fromFunction[BestBlockInserted, BestBlockInserted](identity))(
          _.graphWriteFlow
        )
      )

  val insertBranchFlow: Flow[List[LinkedBlock], BestBlockInserted, Future[ChainSyncResult]] =
    Flow
      .apply[List[LinkedBlock]]
      .flatMapConcat {
        case bestBlock :: Nil =>
          Source.single(bestBlock).via(insertBlockFlow)
        case winningFork =>
          insertForkFlow(winningFork).mapMaterializedValue { loosingFork =>
            backendOpt.fold(Success(NotUsed))(_.removeBlocksFromMainChain(loosingFork.keys).map(_ => NotUsed))
          }
      }
      .wireTap { b =>
        if (b.blockWithInputs.info.height % mvStoreConf.heightCompactRate == 0)
          storageService.compact(indexing = true, mvStoreConf.maxIndexingCompactTime, mvStoreConf.maxIdleCompactTime)
        else Success(())
      }
      .via(backendPersistence)
      .via(graphPersistenceFlow)
      .alsoToMat(Sink.fold(0) { case (count, _) => count + 1 }) { case (_, futureCount) => futureCount }
      .alsoToMat(Sink.lastOption[BestBlockInserted]) { case (totalCountF, lastBlockF) =>
        for {
          lastBlock  <- lastBlockF
          totalCount <- totalCountF
        } yield {
          storageService.writeReportAndCompact(totalCount).fold(_ => NotUsed, _ => NotUsed)
          ChainSyncResult(
            lastBlock,
            storageService.readableStorage,
            graphBackendOpt.map(_.graphTraversalSource)
          )
        }
      }

  def insertForkFlow(winningFork: List[LinkedBlock]): Source[BestBlockInserted, ForkInserted.LoosingFork] =
    rollbackFork(winningFork)
      .fold(
        Source.failed[BestBlockInserted](_).mapMaterializedValue(_ => Map.empty),
        forkInserted =>
          Source(forkInserted.winningFork).via(insertBlockFlow).mapMaterializedValue(_ => forkInserted.loosingFork)
      )

  val insertBlockFlow: Flow[LinkedBlock, BestBlockInserted, NotUsed] =
    Flow
      .apply[LinkedBlock]
      .map(UtxoTracker.getBlockWithInputs(_, storage).get)
      .async
      .wireTap(b => storage.persistErgoTreeUtxos(b.outputRecords).get)
      .wireTap(b => storage.removeInputBoxesByErgoTree(b.inputRecords).get)
      .async
      .wireTap(b => storage.persistErgoTreeTemplateUtxos(b.outputRecords).get)
      .wireTap(b => storage.removeInputBoxesByErgoTreeT8(b.inputRecords).get)
      .wireTap(b => storage.commitNewBlock(b.b.header.id, b.info, storage.getCurrentRevision).get)
      .async
      .map(lb => BestBlockInserted(lb, None))

}
