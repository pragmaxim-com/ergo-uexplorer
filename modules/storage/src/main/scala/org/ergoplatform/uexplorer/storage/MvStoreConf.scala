package org.ergoplatform.uexplorer.storage

import org.ergoplatform.uexplorer.storage.MvStorage.{CacheSize, HeightCompactRate, MaxCompactTime}

case class MvStoreConf(
  cacheSize: CacheSize,
  maxIndexingCompactTime: MaxCompactTime,
  maxIdleCompactTime: MaxCompactTime,
  heightCompactRate: HeightCompactRate
)
