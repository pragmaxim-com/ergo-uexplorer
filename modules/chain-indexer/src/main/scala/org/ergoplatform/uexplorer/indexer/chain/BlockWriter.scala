package org.ergoplatform.uexplorer.indexer.chain

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.backend.Repo
import org.ergoplatform.uexplorer.chain.{BlockProcessor, ChainLinker, ChainTip}
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.indexer.chain.StreamExecutor.ChainSyncResult
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.{CoreConf, ReadableStorage, UnexpectedStateError, WritableStorage}
import zio.*
import zio.Exit.Success
import zio.stream.{ZSink, ZStream}

case class BlockWriter(
  storage: WritableStorage,
  repo: Repo,
  graphBackend: GraphBackend,
  chainIndexerConf: ChainIndexerConf
) {

  implicit private val ps: CoreConf            = chainIndexerConf.core
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
        _              <- ZIO.log(s"Adding fork from height ${winningFork.head.block.height} until ${winningFork.last.block.height}")
        preForkVersion <- ZIO.attempt(storage.getBlockById(winningFork.head.b.header.id).map(_.revision).get)
        loosingFork = winningFork.flatMap(b => storage.getBlocksByHeight(b.block.height).filter(_._1 != b.b.header.id)).toMap
        _ <- ZIO.attempt(storage.rollbackTo(preForkVersion))
      } yield ForkInserted(winningFork, loosingFork)
    }

  def getChainTip: Task[ChainTip] =
    storage.getChainTip.flatMap { chainTip =>
      repo.getLastBlock.flatMap { lastBlock =>
        if (storage.getCurrentRevision > 1 && chainTip.latestBlock.exists(sb => lastBlock.exists(rb => sb.height > rb.height))) {
          ZIO.logWarning(
            s"Rolling back storage to ${storage.getCurrentRevision} due to storage/repo incompatibility"
          ) *> ZIO.attempt(storage.rollbackTo(lastBlock.get.revision + 1)) *> storage.getChainTip
        } else if (chainTip.latestBlock.map(_.blockId) != lastBlock.map(_.blockId)) {
          ZIO.fail(
            new IllegalStateException(
              s"Storage last block ${chainTip.latestBlock.map(_.height)} is not compatible with repo last block ${lastBlock.map(_.height)}, please delete db files and restart"
            )
          )
        } else {
          ZIO.log(s"Chain is valid, continue loading ...") *> ZIO.succeed(chainTip)
        }
      }
    }

  def insertBranchFlow(
    source: ZStream[Any, Throwable, ApiFullBlock],
    chainLinker: ChainLinker
  ): Task[ChainSyncResult] =
    BlockProcessor
      .processingFlow(chainLinker)
      .mapStream[Any, Throwable, BestBlockInserted] {
        case bestBlock :: Nil =>
          ZStream.fromIterable(List(bestBlock)).mapZIO(persistBlock)
        case winningFork =>
          ZStream
            .fromIterableZIO(
              for {
                forkInserted <- rollbackFork(winningFork)
                _            <- repo.removeBlocks(forkInserted.loosingFork.keySet)
              } yield forkInserted.winningFork
            )
            .mapZIO(persistBlock)
      }
      .tap { b =>
        if (b.linkedBlock.block.height % chainIndexerConf.mvStore.heightCompactRate == 0)
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
      .onExit {
        case Success(Some((lastBlock, indexCount))) =>
          ZIO.log(s"Writing report after block at height ${lastBlock.linkedBlock.block.height}, indexed $indexCount blocks ...") *> storage
            .writeReportAndCompact(false)
            .orDie
        case _ =>
          ZIO.log(s"Writing report ...") *> storage.writeReportAndCompact(false).orDie
      }
      .map { lastBlock =>
        ChainSyncResult(
          lastBlock.map(_._1),
          storage.asInstanceOf[ReadableStorage],
          graphBackend.graphTraversalSource
        )
      }

  private def persistBlock(b: LinkedBlock): Task[BestBlockInserted] =
    repo
      .writeBlock(b)(
        preTx = ZIO.collectAllParDiscard(
          List(
            storage.persistErgoTreeByUtxo(b.outputRecords) *> storage.removeInputBoxesByErgoTree(b.b.transactions.transactions),
            storage.persistErgoTreeT8ByUtxo(b.outputRecords) *> storage.removeInputBoxesByErgoTreeT8(b.b.transactions.transactions),
            storage.persistUtxosByTokenId(b.outputRecords.utxosByTokenId) *> storage.persistTokensByUtxo(b.outputRecords.tokensByUtxo) *> storage
              .removeInputBoxesByTokenId(b.b.transactions.transactions),
            storage.insertNewBlock(b.b.header.id, b.block, storage.getCurrentRevision)
          )
        ),
        postTx = ZIO.attempt(storage.commit())
      )
      .as(BestBlockInserted(b, None))

}

object BlockWriter {
  def layer: ZLayer[
    WritableStorage with Repo with GraphBackend with ChainIndexerConf,
    Nothing,
    BlockWriter
  ] =
    ZLayer.fromFunction(BlockWriter.apply _)

}
