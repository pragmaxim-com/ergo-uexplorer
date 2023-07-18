package org.ergoplatform.uexplorer.storage

import org.ergoplatform.uexplorer.config.ExplorerConfig
import org.ergoplatform.uexplorer.mvstore.*
import zio.{IO, ZLayer}
import zio.config.magnolia.deriveConfig

case class MvStoreConf(
  cacheSize: CacheSize,
  maxIndexingCompactTime: MaxCompactTime,
  maxIdleCompactTime: MaxCompactTime,
  heightCompactRate: HeightCompactRate
)

object MvStoreConf {

  val config: zio.Config[MvStoreConf] =
    deriveConfig[MvStoreConf].nested("mvStore")

  def configIO: IO[zio.Config.Error, MvStoreConf] = ExplorerConfig().load[MvStoreConf](config)

  def layer: ZLayer[Any, zio.Config.Error, MvStoreConf] = ZLayer.fromZIO(configIO)

}
