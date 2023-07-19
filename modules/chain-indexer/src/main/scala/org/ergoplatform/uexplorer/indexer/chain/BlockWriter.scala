package org.ergoplatform.uexplorer.indexer.chain

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.ExeContext.Implicits
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.indexer.chain.StreamExecutor.ChainSyncResult
import org.ergoplatform.uexplorer.indexer.db.Backend
import org.ergoplatform.uexplorer.storage.MvStorage
import org.ergoplatform.uexplorer.{ProtocolSettings, ReadableStorage, UnexpectedStateError, WritableStorage}
import zio.*
import zio.prelude.CommutativeBothOps

import java.util.concurrent.Flow.Processor
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import zio.stream.ZPipeline
import zio.stream.ZStream
import zio.stream.ZSink
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.chain.ChainLinker
import org.ergoplatform.uexplorer.backend.Repo
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf

case class BlockWriter(
  storage: WritableStorage,
  repo: Repo,
  graphBackend: GraphBackend,
  chainIndexerConf: ChainIndexerConf
) {

  implicit private val ps: ProtocolSettings    = chainIndexerConf.protocol
  implicit private val enc: ErgoAddressEncoder = ps.addressEncoder

  private def hasParentAndIsChained(fork: List[LinkedBlock]): Boolean =
    fork.size > 1 && storage.containsBlock(fork.head.block.parentId, fork.head.block.height - 1) &&
      fork.sliding(2).forall {
        case first :: second :: Nil =>
          first.b.header.id == second.block.parentId
        case _ =>
          false
      }

  private def rollbackFork(winningFork: List[LinkedBlock]): Task[ForkInserted] =
    if (!hasParentAndIsChained(winningFork)) {
      ZIO.fail(
        new UnexpectedStateError(
          s"Inserting fork ${winningFork.map(_.b.header.id).mkString(",")} at height ${winningFork.map(_.block.height).mkString(",")} illegal"
        )
      )
    } else {
      for {
        _ <- ZIO.log(s"Adding fork from height ${winningFork.head.block.height} until ${winningFork.last.block.height}")
        preForkVersion <- ZIO.attempt(storage.getBlockById(winningFork.head.b.header.id).map(_.revision).get)
        loosingFork = winningFork.flatMap(b => storage.getBlocksByHeight(b.block.height).filter(_._1 != b.b.header.id)).toMap
        _ <- ZIO.attempt(storage.rollbackTo(preForkVersion))
      } yield ForkInserted(winningFork, loosingFork)
    }

  def insertBranchFlow(
    source: ZStream[Any, Throwable, ApiFullBlock],
    chainLinker: ChainLinker
  ): Task[ChainSyncResult] =
    BlockProcessor
      .processingFlow(chainLinker)
      .mapStream[Any, Throwable, BestBlockInserted] {
        case bestBlock :: Nil =>
          ZStream.fromIterable(List(bestBlock)).via(insertBlockFlow)
        case winningFork =>
          ZStream
            .fromIterableZIO(
              for {
                forkInserted <- rollbackFork(winningFork)
                _            <- repo.removeBlocks(forkInserted.loosingFork.keySet)
              } yield forkInserted.winningFork
            )
            .via(insertBlockFlow)
      }
      .tap { b =>
        if (b.normalizedBlock.block.height % chainIndexerConf.mvStore.heightCompactRate == 0)
          storage.compact(indexing = true)
        else ZIO.succeed(())
      }
      .tap { block =>
        graphBackend.writeTxsAndCommit(block)
      }
      .apply(source)
      .run(
        ZSink.foldLeftChunks[BestBlockInserted, Option[(BestBlockInserted, Int)]](Option.empty) {
          case (prev @ Some(last, count), in) =>
            in.lastOption.map(x => (x, count + 1)).orElse(prev)
          case (None, in) =>
            in.lastOption.map(_ -> 1)
        }
      )
      .tap { lastBlock =>
        storage.writeReportAndCompact(lastBlock.map(_._2).getOrElse(0))
      }
      .map { lastBlock =>
        ChainSyncResult(
          lastBlock.map(_._1),
          storage.asInstanceOf[ReadableStorage],
          graphBackend.graphTraversalSource
        )
      }

  private def persistBlock(b: NormalizedBlock): Task[BestBlockInserted] =
    repo
      .writeBlock(b)(
        preTx = ZIO.collectAllParDiscard(
          List(
            storage.persistErgoTreeUtxos(b.outputRecords) *> storage.removeInputBoxesByErgoTree(b.inputRecords),
            storage.persistErgoTreeT8Utxos(b.outputRecords) *> storage.removeInputBoxesByErgoTreeT8(b.inputRecords),
            storage.insertNewBlock(b.b.header.id, b.block, storage.getCurrentRevision)
          )
        ),
        postTx = ZIO.attempt(storage.commit())
      )
      .as(BestBlockInserted(b, None))

  val insertBlockFlow: ZPipeline[Any, Throwable, LinkedBlock, BestBlockInserted] =
    ZPipeline
      .mapZIOPar(1)(UtxoTracker.getBlockWithInputs(_, storage))
      .mapZIOPar(1)(persistBlock)

}

object BlockWriter {
  def layer: ZLayer[
    WritableStorage with Repo with GraphBackend with ChainIndexerConf,
    Nothing,
    BlockWriter
  ] =
    ZLayer.fromFunction(BlockWriter.apply _)

}
