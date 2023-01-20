package org.ergoplatform.uexplorer.indexer.utxo

import com.datastax.oss.driver.shaded.guava.common.collect.MinMaxPriorityQueue
import org.ergoplatform.uexplorer.{Address, Const, Height}
import org.ergoplatform.uexplorer.indexer.utxo.TopAddresses.{BoxCount, TopAddressMap}

import scala.jdk.CollectionConverters.*
import java.util.Comparator
import scala.collection.mutable
import org.ergoplatform.uexplorer.indexer.MapPimp

case class TopAddresses(
  nodeMap: TopAddressMap,
  maximumSize: Int    = 50 * 1000,
  dropHeightDiff: Int = Const.EpochLength * 50,
  dropBoxCount: Int   = 5000
) {

  def sortedByBoxCount: Seq[(Address, (Height, BoxCount))] =
    nodeMap.toSeq.sortBy(_._2._2)

  def addOrUpdate(height: Int, newNodeMap: TopAddressMap): TopAddresses = {
    val totalSize = nodeMap.size
    if (totalSize > maximumSize) {
      val toRemove =
        nodeMap.iterator
          .collect {
            case (address, (h, c)) if c < dropBoxCount && height - h > dropHeightDiff =>
              address
          }
          .take(totalSize - maximumSize)
      TopAddresses(nodeMap = newNodeMap.removedAll(toRemove))
    } else {
      TopAddresses(nodeMap = newNodeMap)
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
