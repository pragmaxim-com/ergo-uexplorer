package org.ergoplatform.uexplorer.indexer.progress

import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.circe.parser._
import org.ergoplatform.explorer.BlockId
import org.ergoplatform.explorer.protocol.models.ApiFullBlock
import org.ergoplatform.explorer.settings.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import org.ergoplatform.uexplorer.indexer.progress.ProgressState._
import org.ergoplatform.uexplorer.indexer.{Rest, UnexpectedStateError}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.TreeMap

class ProgressStateSpec extends AnyFreeSpec with Matchers with DiffShouldMatcher {

  private def emptyState: ProgressState =
    ProgressState(TreeMap.empty, TreeMap.empty, BlockCache(Map.empty, TreeMap.empty))
  implicit private val protocol: ProtocolSettings = ChainIndexerConf.loadDefaultOrThrow.protocol

  private def getBlock(height: Int): ApiFullBlock =
    parse(Rest.blocks.byHeight(height)).flatMap(_.as[ApiFullBlock]).right.get

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
          val e0b1     = getBlock(1023)
          val e0b2     = getBlock(1024)
          val e1b1     = getBlock(2047)
          val e1b2     = getBlock(2048)
          val e0b2Info = buildBlock(e0b2, Option(buildBlock(e0b1, None).get.info)).get.buildInfo
          val e1b2Info = buildBlock(e1b2, Option(buildBlock(e1b1, None).get.info)).get.buildInfo

          val lastBlockIdByEpochIndex = TreeMap(0 -> e0b2Info, 1 -> e1b2Info)

          emptyState.updateState(lastBlockIdByEpochIndex) shouldBe ProgressState(
            lastBlockIdByEpochIndex.mapValues(_.stats.headerId),
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
          val firstFlatBlock            = buildBlock(firstApiBlock, None).get
          val (blockInserted, newState) = emptyState.insertBestBlock(firstApiBlock).get
          blockInserted.flatBlock shouldBe firstFlatBlock
          newState shouldBe ProgressState(
            TreeMap.empty,
            TreeMap.empty,
            BlockCache(
              Map(firstApiBlock.header.id -> firstFlatBlock.buildInfo),
              TreeMap(1                   -> firstFlatBlock.buildInfo)
            )
          )
        }
        "after an existing block" in {
          val e0b1                    = getBlock(1024)
          val e0b1Info                = buildBlock(e0b1, None).get.buildInfo
          val lastBlockIdByEpochIndex = TreeMap(0 -> e0b1Info)
          val newState                = emptyState.updateState(lastBlockIdByEpochIndex)
          newState shouldBe ProgressState(
            lastBlockIdByEpochIndex.mapValues(_.stats.headerId),
            TreeMap.empty,
            BlockCache(
              Map(e0b1.header.id -> e0b1Info),
              TreeMap(1024       -> e0b1Info)
            )
          )

          val e1b1                       = getBlock(1025)
          val e1b1Block                  = buildBlock(e1b1, Some(e0b1Info.stats)).get
          val e1b1Info                   = BlockInfo(e1b1.header.parentId, e1b1Block.info)
          val (blockInserted, newState2) = newState.insertBestBlock(e1b1).get
          blockInserted.flatBlock shouldBe e1b1Block
          newState2 shouldBe ProgressState(
            TreeMap(0 -> e0b1Info.stats.headerId),
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
        val commonFlatBlock = buildBlock(commonBlock, None).get
        val s               = emptyState.updateState(TreeMap(0 -> commonFlatBlock.buildInfo))
        val b1              = getBlock(1025)
        val b1FlatBlock     = buildBlock(b1, Option(commonFlatBlock.info)).get
        val b2              = getBlock(1026)
        val b2FlatBlock     = buildBlock(b2, Option(b1FlatBlock.info)).get
        val b3              = getBlock(1027)
        val b3FlatBlock     = buildBlock(b3, Option(b2FlatBlock.info)).get
        val b1Fork          = forkBlock(b1, "7975b60515b881504ec471affb84234123ac5491d0452da0eaf5fb96948f18e7")
        val b1ForkFlatBlock = buildBlock(b1Fork, Option(commonFlatBlock.info))
        val b2Fork =
          forkBlock(b2, "4077fcf3359c15c3ad3797a78fff342166f09a7f1b22891a18030dcd8604b087", Option(b1Fork.header.id))
        val b2ForkFlatBlock           = buildBlock(b2Fork, Option(b1ForkFlatBlock.get.info)).get
        val (_, s2)                   = s.insertBestBlock(b1Fork).get
        val (_, s3)                   = s2.insertBestBlock(b2Fork).get
        val (forkInserted, newState4) = s3.insertWinningFork(List(b1, b2, b3)).get

        forkInserted.newFork.size shouldBe 3
        forkInserted.supersededFork.size shouldBe 2
        forkInserted.newFork shouldBe List(b1FlatBlock, b2FlatBlock, b3FlatBlock)
        forkInserted.supersededFork shouldBe List(b1ForkFlatBlock.get.buildInfo, b2ForkFlatBlock.buildInfo)
        newState4 shouldBe ProgressState(
          TreeMap(0 -> commonBlock.header.id),
          TreeMap.empty,
          BlockCache(
            Map(
              commonBlock.header.id -> commonFlatBlock.buildInfo,
              b1.header.id          -> b1FlatBlock.buildInfo,
              b2.header.id          -> b2FlatBlock.buildInfo,
              b3.header.id          -> b3FlatBlock.buildInfo
            ),
            TreeMap(
              1024 -> commonFlatBlock.buildInfo,
              1025 -> b1FlatBlock.buildInfo,
              1026 -> b2FlatBlock.buildInfo,
              1027 -> b3FlatBlock.buildInfo
            )
          )
        )
      }
    }
}
