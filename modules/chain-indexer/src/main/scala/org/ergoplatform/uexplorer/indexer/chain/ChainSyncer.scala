package org.ergoplatform.uexplorer.indexer.chain

import akka.Done
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.pattern.StatusReply
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId}
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.config.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.chain.ChainState.*
import org.ergoplatform.uexplorer.node.ApiFullBlock

import scala.collection.immutable.TreeMap
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

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
        val (maybeNewEpoch, newChainState) = s.finishEpoch(epochIndex)
        logger.info(s"$maybeNewEpoch, $newChainState")
        replyTo ! maybeNewEpoch
        initialized(newChainState)
      case unexpected =>
        logger.error(s"Message $unexpected unexpected")
        Behaviors.same
    }
}

object ChainSyncer {

  implicit private val timeout: Timeout = 3.seconds

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

  case class FinishEpoch(epochIndex: Int, replyTo: ActorRef[MaybeNewEpoch]) extends ChainSyncerRequest

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

  case class NewEpochFailed(epochCandidate: InvalidEpochCandidate) extends MaybeNewEpoch {

    override def toString: String =
      s"New epoch ${epochCandidate.epochIndex} failed due to : ${epochCandidate.error}"
  }

  case class NewEpochExisted(epochIndex: Int) extends MaybeNewEpoch {

    override def toString: String =
      s"Epoch $epochIndex already existed"
  }

  /** API */
  import akka.actor.typed.scaladsl.AskPattern._

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
    ref.ask(ref => FinishEpoch(epochIndex, ref))
}
