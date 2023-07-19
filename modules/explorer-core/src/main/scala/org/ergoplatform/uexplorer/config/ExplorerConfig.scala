package org.ergoplatform.uexplorer.config

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import zio.config.typesafe.TypesafeConfigProvider
import zio.{ConfigProvider, Task, ZIO}

import java.io.File
import scala.jdk.CollectionConverters.*

object ExplorerConfig {
  private val confWhiteList = Set("datastax-java-driver", "h2", "uexplorer")

  def provider(debugLog: Boolean, externalConfPath: File = new File("conf/chain-indexer.conf")): Task[ConfigProvider] =
    ZIO
      .attempt {
        ConfigFactory
          .parseFile(externalConfPath)
          .withFallback(ConfigFactory.load())
          .resolve()
      }
      .tap {
        case rootConfig if debugLog =>
          val formatting = ConfigRenderOptions.concise().setFormatted(true).setJson(true)
          ZIO.log(
            rootConfig
              .entrySet()
              .asScala
              .collect {
                case e if confWhiteList.exists(k => e.getKey.startsWith(k)) =>
                  s"${e.getKey} : ${e.getValue.render(formatting)}"
              }
              .toList
              .sorted
              .mkString("\n", "\n", "\n")
          )
        case _ =>
          ZIO.unit
      }
      .map { rootConfig =>
        TypesafeConfigProvider.fromTypesafeConfig(
          rootConfig.getConfig("uexplorer.chainIndexer"),
          enableCommaSeparatedValueAsList = true
        )
      }

}
