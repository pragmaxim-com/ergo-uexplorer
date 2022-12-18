package org.ergoplatform.uexplorer.indexer.chain

import akka.Done
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.pattern.StatusReply
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId, Const}
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.Indexer.ChainSyncResult
import org.ergoplatform.uexplorer.indexer.UnexpectedStateError
import org.ergoplatform.uexplorer.indexer.api.Backend
import org.ergoplatform.uexplorer.indexer.config.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.chain.ChainState.*
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient
import org.ergoplatform.uexplorer.indexer.utxo.UtxoSnapshotManager
import org.ergoplatform.uexplorer.node.ApiFullBlock

import scala.collection.immutable.{TreeMap, TreeSet}
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

class ChainSyncer(implicit protocol: ProtocolSettings) extends LazyLogging {
  import ChainSyncer._

  def initialBehavior: Behavior[ChainSyncerRequest] =
    Behaviors.setup[ChainSyncerRequest] { _ =>
      Behaviors.receiveMessage[ChainSyncerRequest] {
        case Initialize(newState, replyTo) =>
          replyTo ! Done
          initialized(newState)
        case unexpected =>
          logger.error(s"Message $unexpected unexpected")
          Behaviors.same
      }
    }

  def initialized(s: ChainState): Behaviors.Receive[ChainSyncerRequest] =
    Behaviors.receiveMessage[ChainSyncerRequest] {
      case InsertBestBlock(bestBlock, replyTo) =>
        s.insertBestBlock(bestBlock) match {
          case Success((bestBlockInserted, newChainState)) =>
            replyTo ! StatusReply.success(bestBlockInserted)
            initialized(newChainState)
          case Failure(ex) =>
            val h = bestBlock.header
            logger.warn(s"Unexpected insert ${h.id} at ${h.height}, parent ${h.parentId} : $s", ex)
            replyTo ! StatusReply.error(ex)
            Behaviors.same
        }
      case InsertWinningFork(fork, replyTo) =>
        s.insertWinningFork(fork) match {
          case Success((winningForkInserted, newChainState)) =>
            replyTo ! StatusReply.success(winningForkInserted)
            initialized(newChainState)
          case Failure(ex) =>
            val h = fork.head.header
            logger.warn(
              s"Unexpected fork size ${fork.size} starting ${h.id} at ${h.height}, parent ${h.parentId} : $s",
              ex
            )
            replyTo ! StatusReply.error(ex)
            Behaviors.same
        }
      case GetBlock(blockId, replyTo) =>
        replyTo ! IsBlockCached(s.blockBuffer.byId.contains(blockId))
        Behaviors.same
      case GetChainState(replyTo) =>
        replyTo ! s
        Behaviors.same
      case FinishEpoch(epochIndex, replyTo) =>
        s.finishEpoch(epochIndex) match {
          case Success((maybeNewEpoch, newChainState)) =>
            logger.info(s"$maybeNewEpoch, $newChainState")
            replyTo ! StatusReply.success(maybeNewEpoch)
            initialized(newChainState)
          case Failure(ex) =>
            logger.error(s"Unable to finish epoch due to", ex)
            replyTo ! StatusReply.error(ex)
            Behaviors.same
        }
      case unexpected =>
        logger.error(s"Message $unexpected unexpected")
        Behaviors.same
    }
}

object ChainSyncer extends LazyLogging {

  implicit private val timeout: Timeout = 10.seconds

  /** REQUEST */
  sealed trait ChainSyncerRequest

  sealed trait Insertable extends ChainSyncerRequest {
    def replyTo: ActorRef[StatusReply[Inserted]]
  }

  case class InsertBestBlock(block: ApiFullBlock, replyTo: ActorRef[StatusReply[Inserted]]) extends Insertable

  case class InsertWinningFork(blocks: List[ApiFullBlock], replyTo: ActorRef[StatusReply[Inserted]]) extends Insertable

  case class GetBlock(blockId: BlockId, replyTo: ActorRef[IsBlockCached]) extends ChainSyncerRequest

  case class Initialize(chainState: ChainState, replyTo: ActorRef[Done]) extends ChainSyncerRequest

  case class GetChainState(replyTo: ActorRef[ChainState]) extends ChainSyncerRequest

  case class FinishEpoch(epochIndex: Int, replyTo: ActorRef[StatusReply[MaybeNewEpoch]]) extends ChainSyncerRequest

  /** RESPONSE */
  sealed trait ChainSyncerResponse

  case class IsBlockCached(present: Boolean) extends ChainSyncerResponse

  sealed trait Inserted extends ChainSyncerResponse

  case class BestBlockInserted(flatBlock: Block) extends Inserted

  case class ForkInserted(newFork: List[Block], supersededFork: List[BufferedBlockInfo]) extends Inserted

  sealed trait MaybeNewEpoch extends ChainSyncerResponse

  case class NewEpochCreated(epoch: Epoch) extends MaybeNewEpoch {

    override def toString: String =
      s"New epoch ${epoch.index} created"
  }

  case class NewEpochExisted(epochIndex: Int) extends MaybeNewEpoch {

    override def toString: String =
      s"Epoch $epochIndex already existed"
  }

  /** API */
  import akka.actor.typed.scaladsl.AskPattern._

  trait ChainIntegrity
  case object ChainValid extends ChainIntegrity

  case class MissingEpochs(missingEpochIndexes: TreeSet[Int]) extends ChainIntegrity {
    def missingHeights: TreeSet[Int] = missingEpochIndexes.flatMap(Epoch.heightRangeForEpochIndex)
  }

  private def verifyStateIntegrity(
    chainState: ChainState,
    snapshotManager: UtxoSnapshotManager
  ): Future[ChainIntegrity] = {
    val missingEpochIndexes = chainState.findMissingEpochIndexes
    if (missingEpochIndexes.nonEmpty) {
      logger.error(s"Going to index missing blocks for epochs : ${missingEpochIndexes.mkString(", ")}")
      Future(snapshotManager.clearAllSnapshots).map(_ => MissingEpochs(missingEpochIndexes))
    } else if (chainState.utxoState.utxosByAddress.contains(Const.FeeContract.address)) {
      Future(snapshotManager.clearAllSnapshots).flatMap { _ =>
        Future.failed(
          new UnexpectedStateError("UtxoState should not contain Fee Contract address as all such boxes should be spent")
        )
      }
    } else {
      logger.info(s"Chain state is valid")
      Future.successful(ChainValid)
    }
  }

  def initFromDbAndDisk(
    backend: Backend,
    snapshotManager: UtxoSnapshotManager
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[ChainSyncerRequest]): Future[ChainIntegrity] =
    backend.loadBlockInfoByEpochIndex
      .flatMap {
        case blockInfoByEpochIndex if blockInfoByEpochIndex.isEmpty =>
          Future {
            snapshotManager.clearAllSnapshots
            ChainState.empty
          }
        case blockInfoByEpochIndex
            if snapshotManager.latestSerializedSnapshot.exists(_.epochIndex == blockInfoByEpochIndex.lastKey) =>
          snapshotManager.getLatestSnapshotByIndex
            .map { snapshotOpt =>
              ChainState(blockInfoByEpochIndex, snapshotOpt.get.utxoState)
            }
        case blockInfoByEpochIndex =>
          backend.loadUtxoState(blockInfoByEpochIndex.keysIterator).map { utxoState =>
            snapshotManager.clearAllSnapshots
            ChainState(blockInfoByEpochIndex, utxoState)
          }
      }
      .flatMap { chainState =>
        initialize(chainState).flatMap { _ =>
          verifyStateIntegrity(chainState, snapshotManager)
        }
      }

  def syncChain(
    blockHttpClient: BlockHttpClient,
    indexingSink: Sink[Int, Future[ChainSyncResult]]
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[ChainSyncerRequest]): Future[ChainSyncResult] = for {
    bestBlockHeight <- blockHttpClient.getBestBlockHeight
    chainState      <- getChainState
    fromHeight = chainState.getLastCachedBlock.map(_.height).getOrElse(0) + 1
    _          = if (bestBlockHeight > fromHeight) logger.info(s"Going to index blocks from $fromHeight to $bestBlockHeight")
    _          = if (bestBlockHeight == fromHeight) logger.info(s"Going to index block $bestBlockHeight")
    syncResult <- Source(fromHeight to bestBlockHeight).runWith(indexingSink)
  } yield syncResult

  def insertWinningFork(
    winningFork: List[ApiFullBlock]
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[ChainSyncerRequest]): Future[Inserted] =
    ref.askWithStatus(ref => InsertWinningFork(winningFork, ref))

  def insertBestBlock(
    bestBlock: ApiFullBlock
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[ChainSyncerRequest]): Future[Inserted] =
    ref.askWithStatus(ref => InsertBestBlock(bestBlock, ref))

  def containsBlock(
    blockId: BlockId
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[ChainSyncerRequest]): Future[IsBlockCached] =
    ref.ask(ref => GetBlock(blockId, ref))

  def initialize(
    chainState: ChainState
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[ChainSyncerRequest]): Future[Done] =
    ref.ask[Done](ref => Initialize(chainState, ref))(1.minute, s.scheduler)

  def getChainState(implicit s: ActorSystem[Nothing], ref: ActorRef[ChainSyncerRequest]): Future[ChainState] =
    ref.ask(ref => GetChainState(ref))

  def finishEpoch(
    epochIndex: Int
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[ChainSyncerRequest]): Future[MaybeNewEpoch] =
    ref.askWithStatus(ref => FinishEpoch(epochIndex, ref))
}
