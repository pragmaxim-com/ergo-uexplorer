package org.ergoplatform.uexplorer

import org.ergoplatform.settings.MonetarySettings
import org.ergoplatform.mining.emission.EmissionRules
import org.ergoplatform.ErgoAddressEncoder
import pureconfig.ConfigReader
import pureconfig._
import pureconfig.generic.derivation.default._

final case class ProtocolSettings(
                                   networkPrefix: NetworkPrefix,
                                   genesisAddress: Address,
                                 )  derives ConfigReader {

  val monetary = MonetarySettings()
  val emission = new EmissionRules(monetary)

  val addressEncoder: ErgoAddressEncoder =
    ErgoAddressEncoder(networkPrefix.value.toByte)
}
object ProtocolSettings {

  implicit def addrConfigReader: ConfigReader[Address] =
    implicitly[ConfigReader[String]].map(Address.fromStringUnsafe)

  implicit def netConfigReader: ConfigReader[NetworkPrefix] =
    implicitly[ConfigReader[String]].map(NetworkPrefix.fromStringUnsafe)

}
