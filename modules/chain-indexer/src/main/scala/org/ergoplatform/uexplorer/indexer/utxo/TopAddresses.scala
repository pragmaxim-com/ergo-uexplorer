package org.ergoplatform.uexplorer.indexer.utxo

import com.datastax.oss.driver.shaded.guava.common.collect.MinMaxPriorityQueue
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.indexer.utxo.TopAddresses.*

import scala.jdk.CollectionConverters.*
import java.util.Comparator
import scala.collection.immutable.ListMap
import scala.collection.mutable

case class TopAddresses(
  nodeMap: TopAddressMap,
  maximumSize: Int    = 100 * 1000,
  dropHeightDiff: Int = Const.EpochLength * 50,
  dropBoxCount: Int   = 5000,
  dropTxCount: Int    = 100
) {

  def sortedByBoxCount: ListMap[Address, Address.Stats] =
    ListMap
      .newBuilder[Address, Address.Stats]
      .addAll(nodeMap.toSeq.sortBy(_._2.boxCount))
      .result()

  def sortedByTxCount: ListMap[Address, Address.Stats] =
    ListMap
      .newBuilder[Address, Address.Stats]
      .addAll(nodeMap.toSeq.sortBy(_._2.txCount))
      .result()

  def sortedByLastHeight: ListMap[Address, Address.Stats] =
    ListMap
      .newBuilder[Address, Address.Stats]
      .addAll(nodeMap.toSeq.sortBy(_._2.lastTxHeight))
      .result()

  def addOrUpdate(height: Int, newNodeMap: TopAddressMap): TopAddresses = {
    val totalSize = nodeMap.size
    if (totalSize > maximumSize) {
      val toRemove =
        nodeMap.iterator
          .collect {
            case (address, Address.Stats(lastTxHeight, txCount, boxCount))
                if (boxCount < dropBoxCount || txCount < dropTxCount) && height - lastTxHeight > dropHeightDiff =>
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

  def empty: TopAddresses = TopAddresses(nodeMap = Map.empty)

  def from(topAddresses: TopAddressMap): TopAddresses =
    TopAddresses(nodeMap = topAddresses)
}
