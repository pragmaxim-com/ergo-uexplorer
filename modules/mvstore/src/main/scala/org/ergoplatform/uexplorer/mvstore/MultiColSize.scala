package org.ergoplatform.uexplorer.mvstore

case class MultiColSize(superNodeCount: Int, superNodeSum: Int, commonSize: Int) {
  def totalSize: CacheSize = superNodeSum + commonSize

  override def toString: MultiColId = s"(supernodeCount/supernodeSum/common/total): $superNodeCount/$superNodeSum/$commonSize/$totalSize"
    
}
