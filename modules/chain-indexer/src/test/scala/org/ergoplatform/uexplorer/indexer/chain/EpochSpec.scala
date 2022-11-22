package org.ergoplatform.uexplorer.indexer.chain

import org.ergoplatform.uexplorer.indexer.{TestSupport, UnexpectedStateError}
import org.ergoplatform.uexplorer.{BlockId, HexString}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ArraySeq

class EpochSpec extends AnyFreeSpec with TestSupport with Matchers {

  val preGenesisBlockId = BlockId.fromStringUnsafe("0000000000000000000000000000000000000000000000000000000000000000")

  private def epochRelationsByHeight(heightRange: Iterable[Int]) =
    heightRange.foldLeft(Vector.empty[(Int, BlockRel)]) {
      case (acc, height) if acc.isEmpty =>
        acc :+ (height -> BlockRel(idGen.sample.get, preGenesisBlockId))
      case (acc, height) =>
        acc :+ (height -> BlockRel(idGen.sample.get, acc.last._2.headerId))
    }

  "height range should be deduced from epoch index" in {
    Epoch.heightRangeForEpochIndex(0) should be(1 to 1024)
    Epoch.heightRangeForEpochIndex(1) should be(1025 to 2048)
    assertThrows[UnexpectedStateError](Epoch.heightRangeForEpochIndex(-1))
  }

  "epoch index should be deduced from height" in {
    Epoch.epochIndexForHeight(1) shouldBe 0
    Epoch.epochIndexForHeight(1024) shouldBe 0
    Epoch.epochIndexForHeight(1025) shouldBe 1
    assertThrows[UnexpectedStateError](Epoch.epochIndexForHeight(0))
    assertThrows[UnexpectedStateError](Epoch.epochIndexForHeight(-1))
  }

  "epoch constructor should" - {

    "succeed with valid relations and index" in {
      val firstEpoch  = EpochCandidate(epochRelationsByHeight(1 to 1024), ArraySeq.empty, ArraySeq.empty)
      val secondEpoch = EpochCandidate(epochRelationsByHeight(1025 to 2048), ArraySeq.empty, ArraySeq.empty)
      val thirdEpoch  = EpochCandidate(epochRelationsByHeight(2049 to 3072), ArraySeq.empty, ArraySeq.empty)
      firstEpoch.toOption.get.isComplete shouldBe true
      secondEpoch.toOption.get.isComplete shouldBe true
      thirdEpoch.toOption.get.isComplete shouldBe true
    }

    "fail with" - {
      "invalid epoch index" in {
        assertThrows[UnexpectedStateError](
          EpochCandidate(epochRelationsByHeight(-4 to 1019), ArraySeq.empty, ArraySeq.empty)
        )
      }
      "invalid height range size" in {
        val epoch = EpochCandidate(epochRelationsByHeight(2 to 1024), ArraySeq.empty, ArraySeq.empty)
        epoch.swap.toOption.get.isComplete shouldBe false
      }
      "invalid sequence" in {
        val epoch = EpochCandidate(epochRelationsByHeight((512 to 1 by -1) ++ (513 to 1024)), ArraySeq.empty, ArraySeq.empty)
        epoch.swap.toOption.get.isComplete shouldBe false
      }
      "invalid relations" in {
        val invalidRels = (1025 to 2048).map(height => height -> BlockRel(idGen.sample.get, preGenesisBlockId))
        val epoch       = EpochCandidate(invalidRels, ArraySeq.empty, ArraySeq.empty)
        epoch.swap.toOption.get.isComplete shouldBe false
      }
    }

  }

}