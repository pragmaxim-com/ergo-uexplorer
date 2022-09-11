package org.ergoplatform.uexplorer.indexer

import org.ergoplatform.explorer.{BlockId, HexString}
import org.ergoplatform.uexplorer.indexer.progress.{BlockRel, EpochCandidate}
import org.ergoplatform.uexplorer.indexer.commonGenerators._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class EpochScannerSpec extends AnyFreeSpec with Matchers {

  val preGenesisBlockId = BlockId(
    HexString.fromStringUnsafe("0000000000000000000000000000000000000000000000000000000000000000")
  )

  private def epochRelationsByHeight(heightRange: Iterable[Int]) =
    heightRange.foldLeft(Vector.empty[(Int, BlockRel)]) {
      case (acc, height) if acc.isEmpty =>
        acc :+ (height -> BlockRel(idGen.sample.get, preGenesisBlockId))
      case (acc, height) =>
        acc :+ (height -> BlockRel(idGen.sample.get, acc.last._2.headerId))
    }

  "epoch constructor should" - {

    "succeed with valid relations and index" in {
      val firstEpoch  = EpochCandidate(epochRelationsByHeight(1 to 1024))
      val secondEpoch = EpochCandidate(epochRelationsByHeight(1025 to 2048))
      val thirdEpoch  = EpochCandidate(epochRelationsByHeight(2049 to 3072))
      firstEpoch.right.get.isComplete shouldBe true
      secondEpoch.right.get.isComplete shouldBe true
      thirdEpoch.right.get.isComplete shouldBe true
    }

    "fail with" - {
      "invalid epoch index" in {
        val epoch = EpochCandidate(epochRelationsByHeight(-4 to 1019))
        epoch.left.get.isComplete shouldBe false
      }
      "invalid height range size" in {
        val epoch = EpochCandidate(epochRelationsByHeight(2 to 1024))
        epoch.left.get.isComplete shouldBe false
      }
      "invalid sequence" in {
        val epoch = EpochCandidate(epochRelationsByHeight((512 to 1 by -1) ++ (513 to 1024)))
        epoch.left.get.isComplete shouldBe false
      }
      "invalid relations" in {
        val invalidRels = (1025 to 2048).map(height => height -> BlockRel(idGen.sample.get, preGenesisBlockId))
        val epoch       = EpochCandidate(invalidRels)
        epoch.left.get.isComplete shouldBe false
      }
    }

  }

}
