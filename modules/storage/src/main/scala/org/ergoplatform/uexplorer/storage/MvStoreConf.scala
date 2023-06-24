package org.ergoplatform.uexplorer.storage

import org.ergoplatform.uexplorer.mvstore.*

case class MvStoreConf(
  cacheSize: CacheSize,
  maxIndexingCompactTime: MaxCompactTime,
  maxIdleCompactTime: MaxCompactTime,
  heightCompactRate: HeightCompactRate
)
