package org.ergoplatform.uexplorer.indexer.progress

import io.circe.parser._
import org.ergoplatform.explorer.protocol.models.ApiFullBlock
import org.ergoplatform.explorer.settings.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.api.BlockBuilder
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import org.ergoplatform.uexplorer.indexer.progress.ProgressMonitor.{BlockCache, BlockInfo, ProgressState}
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
      "allow for updating epoch indexes" in {
        val e0b1 = getBlock(1023)
        val e0b2 = getBlock(1024)
        val e1b1 = getBlock(2047)
        val e2b2 = getBlock(2048)
        val e0b2Info = BlockInfo(
          e0b1.header.parentId,
          BlockBuilder.buildBlock(e0b2, Option(BlockBuilder.buildBlock(e0b1, None).info)).info
        )
        val e2b2Info = BlockInfo(
          e1b1.header.parentId,
          BlockBuilder.buildBlock(e2b2, Option(BlockBuilder.buildBlock(e1b1, None).info)).info
        )

        val lastBlockIdByEpochIndex = TreeMap(0 -> e0b2Info, 1 -> e2b2Info)

        val newState =
          emptyState.updateEpochIndexes(lastBlockIdByEpochIndex)
        newState shouldBe
        ProgressState(
          lastBlockIdByEpochIndex.mapValues(_.stats.headerId),
          TreeSet.empty,
          BlockCache(
            Map(e0b2.header.id -> e0b2Info, e2b2.header.id -> e2b2Info),
            TreeMap(1024       -> e0b2Info, 2048           -> e2b2Info)
          )
        )
      }

    }
}
