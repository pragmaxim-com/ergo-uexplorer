package org.ergoplatform.uexplorer.indexer.chain

import akka.Done
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.pattern.StatusReply
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.UnexpectedStateError
import org.ergoplatform.uexplorer.indexer.api.Backend
import org.ergoplatform.uexplorer.indexer.chain.ChainState.*
import org.ergoplatform.uexplorer.indexer.config.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.http.BlockHttpClient
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.indexer.utxo.UtxoState.Tx

import scala.collection.immutable.{ArraySeq, TreeMap, TreeSet}
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

class ChainStateHolder(implicit protocol: ProtocolSettings) extends LazyLogging {
  import ChainStateHolder.*

  def initialBehavior: Behavior[ChainStateHolderRequest] =
    Behaviors.setup[ChainStateHolderRequest] { _ =>
      Behaviors.receiveMessage[ChainStateHolderRequest] {
        case Initialize(newState, replyTo) =>
          replyTo ! Done
          initialized(newState)
        case unexpected =>
          logger.error(s"Message $unexpected unexpected")
          Behaviors.same
      }
    }

  def initialized(s: ChainState): Behaviors.Receive[ChainStateHolderRequest] =
    Behaviors.receiveMessage[ChainStateHolderRequest] {
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

object ChainStateHolder extends LazyLogging {

  implicit private val timeout: Timeout = 10.seconds

  /** REQUEST */
  sealed trait ChainStateHolderRequest

  sealed trait Insertable extends ChainStateHolderRequest {
    def replyTo: ActorRef[StatusReply[Inserted]]
  }

  case class InsertBestBlock(block: ApiFullBlock, replyTo: ActorRef[StatusReply[Inserted]]) extends Insertable

  case class InsertWinningFork(blocks: List[ApiFullBlock], replyTo: ActorRef[StatusReply[Inserted]]) extends Insertable

  case class GetBlock(blockId: BlockId, replyTo: ActorRef[IsBlockCached]) extends ChainStateHolderRequest

  case class Initialize(chainState: ChainState, replyTo: ActorRef[Done]) extends ChainStateHolderRequest

  case class GetChainState(replyTo: ActorRef[ChainState]) extends ChainStateHolderRequest

  case class FinishEpoch(epochIndex: Int, replyTo: ActorRef[StatusReply[MaybeNewEpoch]]) extends ChainStateHolderRequest

  /** RESPONSE */
  sealed trait ChainStateHolderResponse

  case class IsBlockCached(present: Boolean) extends ChainStateHolderResponse

  sealed trait Inserted extends ChainStateHolderResponse

  case class BestBlockInserted(flatBlock: Block) extends Inserted

  case class ForkInserted(newFork: List[Block], supersededFork: List[BufferedBlockInfo]) extends Inserted

  sealed trait MaybeNewEpoch extends ChainStateHolderResponse

  case class NewEpochDetected(
    epoch: Epoch,
    boxesByHeight: TreeMap[Int, ArraySeq[(Tx, (ArraySeq[(BoxId, Address, Long)], ArraySeq[(BoxId, Address, Long)]))]]
  ) extends MaybeNewEpoch {

    override def toString: String =
      s"New epoch ${epoch.index} detected"
  }

  case class NewEpochExisted(epochIndex: Int) extends MaybeNewEpoch {

    override def toString: String =
      s"Epoch $epochIndex already existed"
  }

  /** API */
  import akka.actor.typed.scaladsl.AskPattern.*

  def insertWinningFork(
    winningFork: List[ApiFullBlock]
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[ChainStateHolderRequest]): Future[Inserted] =
    ref.askWithStatus(ref => InsertWinningFork(winningFork, ref))

  def insertBestBlock(
    bestBlock: ApiFullBlock
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[ChainStateHolderRequest]): Future[Inserted] =
    ref.askWithStatus(ref => InsertBestBlock(bestBlock, ref))

  def containsBlock(
    blockId: BlockId
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[ChainStateHolderRequest]): Future[IsBlockCached] =
    ref.ask(ref => GetBlock(blockId, ref))

  def initialize(
    chainState: ChainState
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[ChainStateHolderRequest]): Future[Done] =
    ref.ask[Done](ref => Initialize(chainState, ref))(1.minute, s.scheduler)

  def getChainState(implicit s: ActorSystem[Nothing], ref: ActorRef[ChainStateHolderRequest]): Future[ChainState] =
    ref.ask(ref => GetChainState(ref))

  def finishEpoch(
    epochIndex: Int
  )(implicit s: ActorSystem[Nothing], ref: ActorRef[ChainStateHolderRequest]): Future[MaybeNewEpoch] =
    ref.askWithStatus(ref => FinishEpoch(epochIndex, ref))
}
