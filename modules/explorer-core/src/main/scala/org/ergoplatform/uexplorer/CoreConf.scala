package org.ergoplatform.uexplorer

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.mining.emission.EmissionRules
import org.ergoplatform.settings.MonetarySettings
import org.ergoplatform.uexplorer.config.ExplorerConfig
import pureconfig.*
import pureconfig.generic.derivation.default.*
import zio.config.magnolia.{deriveConfig, DeriveConfig}
import zio.{ZIO, ZLayer}

final case class CoreConf(networkPrefix: NetworkPrefix) {

  val monetary = MonetarySettings()
  val emission = new EmissionRules(monetary)

  val addressEncoder: ErgoAddressEncoder =
    ErgoAddressEncoder(networkPrefix.value.toByte)
}

object CoreConf {
  implicit val addressConfig: DeriveConfig[NetworkPrefix] =
    DeriveConfig[String].map(addr => NetworkPrefix.fromStringUnsafe(addr))

  def config: zio.Config[CoreConf] = deriveConfig[CoreConf].nested("core")

  def configIO: ZIO[Any, Throwable, CoreConf] =
    for {
      provider <- ExplorerConfig.provider(debugLog = false)
      conf     <- provider.load[CoreConf](config)
    } yield conf

  def layer: ZLayer[Any, Throwable, CoreConf] = ZLayer.fromZIO(configIO)

}
