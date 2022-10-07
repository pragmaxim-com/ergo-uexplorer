package org.ergoplatform.uexplorer.indexer.progress

import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.circe.parser._
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import org.ergoplatform.uexplorer.indexer.db.BlockBuilder
import org.ergoplatform.uexplorer.indexer.progress.ProgressState._
import org.ergoplatform.uexplorer.indexer.{Rest, UnexpectedStateError}
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.{BlockId, ProtocolSettings}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.TreeMap

class ProgressStateSpec extends AnyFreeSpec with Matchers with DiffShouldMatcher {

  private def emptyState: ProgressState =
    ProgressState(TreeMap.empty, TreeMap.empty, BlockCache(Map.empty, TreeMap.empty))
  implicit private val protocol: ProtocolSettings = ChainIndexerConf.loadDefaultOrThrow.protocol

  private def getBlock(height: Int): ApiFullBlock =
    parse(Rest.blocks.byHeight(height)).flatMap(_.as[ApiFullBlock]).toOption.get

  private def forkBlock(
    apiFullBlock: ApiFullBlock,
    newBlockId: String,
    parentIdOpt: Option[BlockId] = None
  ): ApiFullBlock = {
    import monocle.macros.syntax.lens._
    apiFullBlock
      .lens(_.header.id)
      .modify(_ => BlockId.fromStringUnsafe(newBlockId))
      .lens(_.header.parentId)
      .modify(parentId => parentIdOpt.getOrElse(parentId))
  }

  val lastBlockInfoByEpochIndex =
    "ProgressState state should" - {
      "allow for updating epoch indexes" - {
        "when db has no epochs yet" - {
          emptyState.updateState(TreeMap.empty) shouldBe emptyState
        }
        "when has epochs" - {
          val e0b1 = getBlock(1023)
          val e0b2 = getBlock(1024)
          val e1b1 = getBlock(2047)
          val e1b2 = getBlock(2048)
          val e0b2Info = CachedBlock.fromBlock(
            BlockBuilder(e0b2, Option(CachedBlock.fromBlock(BlockBuilder(e0b1, None).get))).get
          )
          val e1b2Info = CachedBlock.fromBlock(
            BlockBuilder(e1b2, Option(CachedBlock.fromBlock(BlockBuilder(e1b1, None).get))).get
          )

          val lastBlockIdByEpochIndex = TreeMap(0 -> e0b2Info, 1 -> e1b2Info)

          emptyState.updateState(lastBlockIdByEpochIndex) shouldBe ProgressState(
            lastBlockIdByEpochIndex.map { case (k, v) => k -> v.headerId },
            TreeMap.empty,
            BlockCache(
              Map(e0b2.header.id -> e0b2Info, e1b2.header.id -> e1b2Info),
              TreeMap(1024       -> e0b2Info, 2048           -> e1b2Info)
            )
          )
        }
      }
      "throw when inserting block without parent being applied first" in {
        assertThrows[UnexpectedStateError](emptyState.insertBestBlock(getBlock(1025)).get)

      }
      "allow for inserting new block" - {
        "after genesis" in {
          val firstApiBlock             = getBlock(1)
          val firstFlatBlock            = BlockBuilder(firstApiBlock, None).get
          val (blockInserted, newState) = emptyState.insertBestBlock(firstApiBlock).get
          blockInserted.flatBlock shouldBe firstFlatBlock
          newState shouldBe ProgressState(
            TreeMap.empty,
            TreeMap.empty,
            BlockCache(
              Map(firstApiBlock.header.id -> CachedBlock.fromBlock(firstFlatBlock)),
              TreeMap(1                   -> CachedBlock.fromBlock(firstFlatBlock))
            )
          )
        }
        "after an existing block" in {
          val e0b1                    = getBlock(1024)
          val e0b1Info                = CachedBlock.fromBlock(BlockBuilder(e0b1, None).get)
          val lastBlockIdByEpochIndex = TreeMap(0 -> e0b1Info)
          val newState                = emptyState.updateState(lastBlockIdByEpochIndex)
          newState shouldBe ProgressState(
            lastBlockIdByEpochIndex.map { case (k, v) => k -> v.headerId },
            TreeMap.empty,
            BlockCache(
              Map(e0b1.header.id -> e0b1Info),
              TreeMap(1024       -> e0b1Info)
            )
          )

          val e1b1                       = getBlock(1025)
          val e1b1Block                  = BlockBuilder(e1b1, Some(e0b1Info)).get
          val e1b1Info                   = CachedBlock.fromBlock(e1b1Block)
          val (blockInserted, newState2) = newState.insertBestBlock(e1b1).get
          blockInserted.flatBlock shouldBe e1b1Block
          newState2 shouldBe ProgressState(
            TreeMap(0 -> e0b1Info.headerId),
            TreeMap.empty,
            BlockCache(
              Map(e1b1.header.id -> e1b1Info, e0b1.header.id -> e0b1Info),
              TreeMap(1024       -> e0b1Info, 1025           -> e1b1Info)
            )
          )
        }
      }

      "throw when inserting an empty fork, one-sized fork or unchained fork" in {
        assertThrows[UnexpectedStateError](emptyState.insertWinningFork(List.empty).get)
        assertThrows[UnexpectedStateError](emptyState.insertWinningFork(List(getBlock(1024))).get)
        assertThrows[UnexpectedStateError](emptyState.insertWinningFork(List(getBlock(1024), getBlock(1026))).get)

      }
      "allow for inserting new fork" in {
        val commonBlock     = getBlock(1024)
        val commonFlatBlock = BlockBuilder(commonBlock, None).get
        val s               = emptyState.updateState(TreeMap(0 -> CachedBlock.fromBlock(commonFlatBlock)))
        val b1              = getBlock(1025)
        val b1FlatBlock     = BlockBuilder(b1, Option(CachedBlock.fromBlock(commonFlatBlock))).get
        val b2              = getBlock(1026)
        val b2FlatBlock     = BlockBuilder(b2, Option(CachedBlock.fromBlock(b1FlatBlock))).get
        val b3              = getBlock(1027)
        val b3FlatBlock     = BlockBuilder(b3, Option(CachedBlock.fromBlock(b2FlatBlock))).get
        val b1Fork          = forkBlock(b1, "7975b60515b881504ec471affb84234123ac5491d0452da0eaf5fb96948f18e7")
        val b1ForkFlatBlock = BlockBuilder(b1Fork, Option(CachedBlock.fromBlock(commonFlatBlock))).get
        val b2Fork =
          forkBlock(b2, "4077fcf3359c15c3ad3797a78fff342166f09a7f1b22891a18030dcd8604b087", Option(b1Fork.header.id))
        val b2ForkFlatBlock           = BlockBuilder(b2Fork, Option(CachedBlock.fromBlock(b1ForkFlatBlock))).get
        val (_, s2)                   = s.insertBestBlock(b1Fork).get
        val (_, s3)                   = s2.insertBestBlock(b2Fork).get
        val (forkInserted, newState4) = s3.insertWinningFork(List(b1, b2, b3)).get

        forkInserted.newFork.size shouldBe 3
        forkInserted.supersededFork.size shouldBe 2
        forkInserted.newFork shouldBe List(b1FlatBlock, b2FlatBlock, b3FlatBlock)
        forkInserted.supersededFork shouldBe List(
          CachedBlock.fromBlock(b1ForkFlatBlock),
          CachedBlock.fromBlock(b2ForkFlatBlock)
        )
        newState4 shouldBe ProgressState(
          TreeMap(0 -> commonBlock.header.id),
          TreeMap.empty,
          BlockCache(
            Map(
              commonBlock.header.id -> CachedBlock.fromBlock(commonFlatBlock),
              b1.header.id          -> CachedBlock.fromBlock(b1FlatBlock),
              b2.header.id          -> CachedBlock.fromBlock(b2FlatBlock),
              b3.header.id          -> CachedBlock.fromBlock(b3FlatBlock)
            ),
            TreeMap(
              1024 -> CachedBlock.fromBlock(commonFlatBlock),
              1025 -> CachedBlock.fromBlock(b1FlatBlock),
              1026 -> CachedBlock.fromBlock(b2FlatBlock),
              1027 -> CachedBlock.fromBlock(b3FlatBlock)
            )
          )
        )
      }
    }
}
