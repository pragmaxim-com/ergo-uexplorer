package org.ergoplatform.uexplorer.storage

import org.ergoplatform.uexplorer.config.ExplorerConfig
import org.ergoplatform.uexplorer.mvstore.*
import zio.{IO, ZIO, ZLayer}
import zio.config.magnolia.deriveConfig

case class MvStoreConf(
  cacheSize: CacheSize,
  maxIndexingCompactTime: MaxCompactTime,
  maxIdleCompactTime: MaxCompactTime,
  heightCompactRate: HeightCompactRate
)

object MvStoreConf {

  def config: zio.Config[MvStoreConf] = deriveConfig[MvStoreConf].nested("mvStore")

  def configIO: ZIO[Any, Throwable, MvStoreConf] =
    for {
      provider <- ExplorerConfig.provider(debugLog = false)
      conf     <- provider.load[MvStoreConf](config)
    } yield conf

  def layer: ZLayer[Any, Throwable, MvStoreConf] = ZLayer.fromZIO(configIO)

}
