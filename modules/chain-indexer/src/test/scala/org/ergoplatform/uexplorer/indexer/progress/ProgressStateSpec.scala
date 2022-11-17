package org.ergoplatform.uexplorer.indexer.progress

import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.circe.parser.*
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.indexer.config.{ChainIndexerConf, ProtocolSettings}
import org.ergoplatform.uexplorer.indexer.db.BlockBuilder
import org.ergoplatform.uexplorer.indexer.parser.ErgoTreeParser
import org.ergoplatform.uexplorer.indexer.progress.ProgressState.*
import org.ergoplatform.uexplorer.indexer.{Rest, UnexpectedStateError}
import org.ergoplatform.uexplorer.node.ApiFullBlock
import org.ergoplatform.uexplorer.{Address, BlockId}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.TreeMap

class ProgressStateSpec extends AnyFreeSpec with Matchers with DiffShouldMatcher {

  private def emptyState: ProgressState =
    ProgressState(
      TreeMap.empty,
      TreeMap.empty,
      BlockBuffer(Map.empty, TreeMap.empty),
      UtxoState.empty
    )
  implicit private val protocol: ProtocolSettings = ChainIndexerConf.loadDefaultOrThrow.protocol
  implicit private val e: ErgoAddressEncoder      = protocol.addressEncoder

  private def getBlock(height: Int): ApiFullBlock =
    parse(Rest.blocks.byHeight(height)).flatMap(_.as[ApiFullBlock]).toOption.get

  private def forkBlock(
    apiFullBlock: ApiFullBlock,
    newBlockId: String,
    parentIdOpt: Option[BlockId] = None
  ): ApiFullBlock = {
    import monocle.syntax.all._
    apiFullBlock
      .focus(_.header.id)
      .modify(_ => BlockId.fromStringUnsafe(newBlockId))
      .focus(_.header.parentId)
      .modify(parentId => parentIdOpt.getOrElse(parentId))
  }

  val lastBlockInfoByEpochIndex =
    "ProgressState state should" - {
      "allow for updating epoch indexes" - {
        "when db has no epochs yet" in {
          ProgressState.empty shouldBe emptyState
        }
        "when has epochs" in {
          val e0b1      = getBlock(1023)
          val e0b1Block = BlockBuilder(e0b1, None).get
          val e0b2      = getBlock(1024)
          val e0b2Block = BlockBuilder(e0b2, None).get
          val e1b1      = getBlock(2047)
          val e1b1Block = BlockBuilder(e1b1, None).get
          val e1b2      = getBlock(2048)
          val e1b2Block = BlockBuilder(e1b2, None).get
          val e0b2Info = BufferedBlockInfo.fromBlock(
            BlockBuilder(e0b2, Option(BufferedBlockInfo.fromBlock(e0b1Block))).get
          )
          val e1b2Info = BufferedBlockInfo.fromBlock(
            BlockBuilder(e1b2, Option(BufferedBlockInfo.fromBlock(e1b1Block))).get
          )

          val e0InputIds = List(e0b1Block, e0b2Block).flatMap(_.inputs.map(_.boxId))
          val e1InputIds = List(e1b1Block, e1b2Block).flatMap(_.inputs.map(_.boxId))

          val e0OutputIds = List(e0b1Block, e0b2Block).flatMap(_.outputs.map(b => b.boxId -> b.address))
          val e1OutputIds = List(e1b1Block, e1b2Block).flatMap(_.outputs.map(b => b.boxId -> b.address))

          val lastBlockIdByEpochIndex =
            TreeMap(
              0 -> e0b2Info,
              1 -> e1b2Info
            )

          val utxos = (e0OutputIds ++ e1OutputIds)
            .filterNot(b => e0InputIds.contains(b._1) || e1InputIds.contains(b._1))
          val inputsWoAddress = (e0InputIds ++ e1InputIds)
            .filterNot(b => e0OutputIds.map(_._1).contains(b) || e1OutputIds.map(_._1).contains(b))
            .toSet

          val utxoState =
            UtxoState(
              TreeMap.empty,
              utxos.toMap,
              utxos.groupBy(_._2).view.mapValues(_.map(_._1).toSet).toMap,
              inputsWoAddress
            )
          val actualProgressState = ProgressState.load(
            lastBlockIdByEpochIndex,
            utxoState
          )
          actualProgressState shouldBe ProgressState(
            lastBlockIdByEpochIndex.map { case (k, v) => k -> v.headerId },
            TreeMap.empty,
            BlockBuffer(
              Map(e0b2.header.id -> e0b2Info, e1b2.header.id -> e1b2Info),
              TreeMap(1024       -> e0b2Info, 2048           -> e1b2Info)
            ),
            utxoState
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
            BlockBuffer(
              Map(firstApiBlock.header.id -> BufferedBlockInfo.fromBlock(firstFlatBlock)),
              TreeMap(1                   -> BufferedBlockInfo.fromBlock(firstFlatBlock))
            ),
            newState.utxoState
          )
        }
        "after an existing block" in {
          val e0b1                    = getBlock(1024)
          val e0b1Block               = BlockBuilder(e0b1, None).get
          val e0b1Info                = BufferedBlockInfo.fromBlock(e0b1Block)
          val lastBlockIdByEpochIndex = TreeMap(0 -> e0b1Info)
          val utxos = e0b1Block.outputs
            .map(b => b.boxId -> b.address)
            .filterNot(b => e0b1Block.inputs.map(_.boxId).contains(b._1))
          val inputsWoAddress = e0b1Block.inputs
            .map(_.boxId)
            .filterNot(b => e0b1Block.outputs.map(_._1).contains(b))
            .toSet
          val utxoState =
            UtxoState(
              TreeMap.empty,
              utxos.toMap,
              utxos.groupBy(_._2).view.mapValues(_.map(_._1).toSet).toMap,
              inputsWoAddress
            )
          val newState = ProgressState.load(lastBlockIdByEpochIndex, utxoState)
          newState shouldBe ProgressState(
            lastBlockIdByEpochIndex.map { case (k, v) => k -> v.headerId },
            TreeMap.empty,
            BlockBuffer(
              Map(e0b1.header.id -> e0b1Info),
              TreeMap(1024       -> e0b1Info)
            ),
            utxoState
          )

          val e1b1                       = getBlock(1025)
          val e1b1Block                  = BlockBuilder(e1b1, Some(e0b1Info)).get
          val e1b1Info                   = BufferedBlockInfo.fromBlock(e1b1Block)
          val (blockInserted, newState2) = newState.insertBestBlock(e1b1).get
          blockInserted.flatBlock shouldBe e1b1Block
          newState2 shouldBe ProgressState(
            TreeMap(0 -> e0b1Info.headerId),
            TreeMap.empty,
            BlockBuffer(
              Map(e1b1.header.id -> e1b1Info, e0b1.header.id -> e0b1Info),
              TreeMap(1024       -> e0b1Info, 1025           -> e1b1Info)
            ),
            newState2.utxoState
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
        val utxos = commonFlatBlock.outputs
          .map(b => b.boxId -> b.address)
          .filterNot(b => commonFlatBlock.inputs.map(_.boxId).contains(b._1))

        val utxoState =
          UtxoState(
            TreeMap.empty,
            utxos.toMap,
            utxos.groupBy(_._2).view.mapValues(_.map(_._1).toSet).toMap,
            Set.empty
          )
        val s               = ProgressState.load(TreeMap(0 -> BufferedBlockInfo.fromBlock(commonFlatBlock)), utxoState)
        val b1              = getBlock(1025)
        val b1FlatBlock     = BlockBuilder(b1, Option(BufferedBlockInfo.fromBlock(commonFlatBlock))).get
        val b2              = getBlock(1026)
        val b2FlatBlock     = BlockBuilder(b2, Option(BufferedBlockInfo.fromBlock(b1FlatBlock))).get
        val b3              = getBlock(1027)
        val b3FlatBlock     = BlockBuilder(b3, Option(BufferedBlockInfo.fromBlock(b2FlatBlock))).get
        val b1Fork          = forkBlock(b1, "7975b60515b881504ec471affb84234123ac5491d0452da0eaf5fb96948f18e7")
        val b1ForkFlatBlock = BlockBuilder(b1Fork, Option(BufferedBlockInfo.fromBlock(commonFlatBlock))).get
        val b2Fork =
          forkBlock(b2, "4077fcf3359c15c3ad3797a78fff342166f09a7f1b22891a18030dcd8604b087", Option(b1Fork.header.id))
        val b2ForkFlatBlock           = BlockBuilder(b2Fork, Option(BufferedBlockInfo.fromBlock(b1ForkFlatBlock))).get
        val (_, s2)                   = s.insertBestBlock(b1Fork).get
        val (_, s3)                   = s2.insertBestBlock(b2Fork).get
        val (forkInserted, newState4) = s3.insertWinningFork(List(b1, b2, b3)).get
        forkInserted.newFork.size shouldBe 3
        forkInserted.supersededFork.size shouldBe 2
        forkInserted.newFork shouldBe List(b1FlatBlock, b2FlatBlock, b3FlatBlock)
        forkInserted.supersededFork shouldBe List(
          BufferedBlockInfo.fromBlock(b1ForkFlatBlock),
          BufferedBlockInfo.fromBlock(b2ForkFlatBlock)
        )
        newState4 shouldBe ProgressState(
          TreeMap(0 -> commonBlock.header.id),
          TreeMap.empty,
          BlockBuffer(
            Map(
              commonBlock.header.id -> BufferedBlockInfo.fromBlock(commonFlatBlock),
              b1.header.id          -> BufferedBlockInfo.fromBlock(b1FlatBlock),
              b2.header.id          -> BufferedBlockInfo.fromBlock(b2FlatBlock),
              b3.header.id          -> BufferedBlockInfo.fromBlock(b3FlatBlock)
            ),
            TreeMap(
              1024 -> BufferedBlockInfo.fromBlock(commonFlatBlock),
              1025 -> BufferedBlockInfo.fromBlock(b1FlatBlock),
              1026 -> BufferedBlockInfo.fromBlock(b2FlatBlock),
              1027 -> BufferedBlockInfo.fromBlock(b3FlatBlock)
            )
          ),
          newState4.utxoState
        )
      }
    }
}
