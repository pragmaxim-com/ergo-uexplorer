package org.ergoplatform.uexplorer.config

import com.typesafe.config.ConfigFactory
import zio.ConfigProvider
import zio.config.typesafe.TypesafeConfigProvider

import java.io.File

object ExplorerConfig {
  def apply(externalConfPath: File = new File("conf/chain-indexer.conf")): ConfigProvider =
    TypesafeConfigProvider
      .fromTypesafeConfig(
        ConfigFactory
          .parseFile(externalConfPath)
          .withFallback(ConfigFactory.load())
          .resolve()
          .getConfig("uexplorer.chainIndexer"),
        enableCommaSeparatedValueAsList = true
      )

}
