package org.ergoplatform.uexplorer.indexer.progress

import io.circe.parser._
import org.ergoplatform.explorer.protocol.models.ApiFullBlock
import org.ergoplatform.explorer.settings.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.api.BlockBuilder._
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor.{BlockCache, ProgressState}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.{TreeMap, TreeSet}
import scala.io.Source

class ProgressMonitorSpec extends AnyFreeSpec with Matchers {

  def emptyState                          = ProgressState(TreeMap.empty, TreeSet.empty, BlockCache(Map.empty, TreeMap.empty))
  implicit val protocol: ProtocolSettings = ChainIndexerConf.loadDefaultOrThrow.protocol

  def getBlock(height: Int): ApiFullBlock =
    parse(
      Source
        .fromInputStream(Thread.currentThread().getContextClassLoader.getResourceAsStream(s"blocks/$height.json"))
        .mkString
    ).flatMap(_.as[ApiFullBlock]).right.get

  val lastBlockInfoByEpochIndex =
    "Progress monitor state should" - {
      "allow for updating epoch indexes" - {
        "when db has no epochs yet" - {
          emptyState.updateEpochIndexes(TreeMap.empty) shouldBe emptyState
        }
        "when has epochs" - {
          val e0b1     = getBlock(1023)
          val e0b2     = getBlock(1024)
          val e1b1     = getBlock(2047)
          val e1b2     = getBlock(2048)
          val e0b2Info = buildBlock(e0b2, Option(buildBlock(e0b1, None).info)).buildInfo
          val e1b2Info = buildBlock(e1b2, Option(buildBlock(e1b1, None).info)).buildInfo

          val lastBlockIdByEpochIndex = TreeMap(0 -> e0b2Info, 1 -> e1b2Info)

          emptyState.updateEpochIndexes(lastBlockIdByEpochIndex) shouldBe ProgressState(
            lastBlockIdByEpochIndex.mapValues(_.stats.headerId),
            TreeSet.empty,
            BlockCache(
              Map(e0b2.header.id -> e0b2Info, e1b2.header.id -> e1b2Info),
              TreeMap(1024       -> e0b2Info, 2048           -> e1b2Info)
            )
          )
        }
      }
      "allow for inserting new block" - {
        "after genesis" in {
          val firstApiBlock             = getBlock(1)
          val firstFlatBlock            = buildBlock(firstApiBlock, None)
          val (blockInserted, newState) = emptyState.insertBestBlock(firstApiBlock)
          blockInserted.flatBlock shouldBe firstFlatBlock
          newState shouldBe ProgressState(
            TreeMap.empty,
            TreeSet.empty,
            BlockCache(
              Map(firstApiBlock.header.id -> firstFlatBlock.buildInfo),
              TreeMap(1                   -> firstFlatBlock.buildInfo)
            )
          )
        }
        "after an existing block" in {
          val e0b1                    = getBlock(1024)
          val e0b1Info                = buildBlock(e0b1, None).buildInfo
          val lastBlockIdByEpochIndex = TreeMap(0 -> e0b1Info)
          val newState                = emptyState.updateEpochIndexes(lastBlockIdByEpochIndex)
          newState shouldBe ProgressState(
            lastBlockIdByEpochIndex.mapValues(_.stats.headerId),
            TreeSet.empty,
            BlockCache(
              Map(e0b1.header.id -> e0b1Info),
              TreeMap(1024       -> e0b1Info)
            )
          )

          val e1b1                       = getBlock(1025)
          val e1b1Block                  = buildBlock(e1b1, Some(e0b1Info.stats))
          val e1b1Info                   = BlockInfo(e1b1.header.parentId, e1b1Block.info)
          val (blockInserted, newState2) = newState.insertBestBlock(e1b1)
          blockInserted.flatBlock shouldBe e1b1Block
          newState2 shouldBe ProgressState(
            TreeMap(0 -> e0b1Info.stats.headerId),
            TreeSet.empty,
            BlockCache(
              Map(e1b1.header.id -> e1b1Info, e0b1.header.id -> e0b1Info),
              TreeMap(1024       -> e0b1Info, 1025           -> e1b1Info)
            )
          )
        }
      }
    }
}
