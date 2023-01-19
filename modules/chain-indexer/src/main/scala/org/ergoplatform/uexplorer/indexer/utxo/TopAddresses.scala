package org.ergoplatform.uexplorer.indexer.utxo

import com.datastax.oss.driver.shaded.guava.common.collect.MinMaxPriorityQueue
import org.ergoplatform.uexplorer.{Address, Const, Height}
import org.ergoplatform.uexplorer.indexer.utxo.TopAddresses.{BoxCount, TopAddressMap}

import scala.jdk.CollectionConverters.*
import java.util.Comparator
import scala.collection.immutable.ListMap

case class TopAddresses(
  maximumSize: Int    = 100 * 1000,
  dropHeightDiff: Int = Const.EpochLength * 100,
  dropBoxCount: Int   = 2000,
  nodeMap: TopAddressMap
) {

  def sortedByBoxCount: Seq[(Address, (Height, BoxCount))] =
    nodeMap.toSeq.sortBy(_._2._2)

  def addOrUpdate(address: Address, height: Height, count: BoxCount): TopAddresses = {
    assert(count > 0, "Do not update counter with 0")
    val newTopAddresses =
      nodeMap.get(address) match {
        case None =>
          nodeMap.updated(address, height -> count)
        case Some((_, oldCount)) =>
          val newCount = oldCount + count
          nodeMap.updated(address, height -> newCount)
      }
    lazy val totalSize = nodeMap.size
    if (height % Const.EpochLength == 0 && totalSize > maximumSize) {
      val toRemove =
        nodeMap.iterator
          .collect {
            case (address, (h, c)) if c < dropBoxCount && height - h > dropHeightDiff =>
              address
          }
          .take(totalSize - maximumSize)
      TopAddresses(nodeMap = newTopAddresses.removedAll(toRemove))
    } else {
      TopAddresses(nodeMap = newTopAddresses)
    }
  }
}

object TopAddresses {

  type BoxCount      = Int
  type TopAddressMap = Map[Address, (Height, BoxCount)]

  def empty: TopAddresses = TopAddresses(nodeMap = Map.empty)

  def from(topAddresses: TopAddressMap): TopAddresses =
    TopAddresses(nodeMap = topAddresses)
}
