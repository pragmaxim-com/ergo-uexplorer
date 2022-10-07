package org.ergoplatform.uexplorer

import cats.syntax.list._
import cats.data.NonEmptyList
import eu.timepit.refined.api.{Refined, Validate}
import eu.timepit.refined.refineV
import eu.timepit.refined.string._
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.mining.emission.EmissionRules
import org.ergoplatform.settings.MonetarySettings
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert
import pureconfig.generic.auto._

final case class ProtocolSettings(
  networkPrefix: String Refined ValidByte,
  genesisAddress: Address,
  monetary: MonetarySettings
) {

  val emission = new EmissionRules(monetary)

  val addressEncoder: ErgoAddressEncoder =
    ErgoAddressEncoder(networkPrefix.value.toByte)
}

object ProtocolSettings {

  implicit def configReaderForRefined[A: ConfigReader, P](implicit
    v: Validate[A, P]
  ): ConfigReader[A Refined P] =
    ConfigReader[A].emap { a =>
      refineV[P](a).left.map(r => CannotConvert(a.toString, s"Refined", r))
    }

  implicit def nelReader[A: ConfigReader]: ConfigReader[NonEmptyList[A]] =
    implicitly[ConfigReader[List[A]]].emap { list =>
      list.toNel.toRight(CannotConvert(list.toString, s"NonEmptyList", "List is empty"))
    }

  implicit def protoSettings: ConfigReader[ProtocolSettings] = implicitly[ConfigReader[ProtocolSettings]]
}
