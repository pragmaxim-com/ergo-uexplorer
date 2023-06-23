package org.ergoplatform.uexplorer

import org.ergoplatform.uexplorer.*
import org.ergoplatform.{ErgoAddressEncoder, ErgoScriptPredef}
import org.ergoplatform.uexplorer.Const.Protocol.Emission
import org.ergoplatform.uexplorer.parser.ErgoTreeParser
import scorex.util.encode.{Base16, Base58}

import scala.util.Try

object Const {

  object Protocol {

    val blockId = BlockId.fromStringUnsafe("0000000000000000000000000000000000000000000000000000000000000000")

    object Emission {
      val tx                     = TxId("4c6282be413c6e300a530618b37790be5f286ded758accc2aebd41554a1be308")
      val initialNanoErgs: Value = (93409132.5d * CoinsInOneErgo).toLong
      val inputBox: BoxId        = BoxId("b69575e11c5c43400bfead5976ee0d6245a1168396b2e2a4f384691f275d501c")
      val outputBox: BoxId       = BoxId("71bc9534d4a4fe8ff67698a5d0f29782836970635de8418da39fee1cd964fcbe")

      val address: Address = Address.fromStringUnsafe(
        "2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU"
      )
      val ergoTree: ErgoTreeHex = ErgoTreeHex.fromStringUnsafe(
        "101004020e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a7017300730110010204020404040004c0fd4f05808c82f5f6030580b8c9e5ae040580f882ad16040204c0944004c0f407040004000580f882ad16d19683030191a38cc7a7019683020193c2b2a57300007473017302830108cdeeac93a38cc7b2a573030001978302019683040193b1a5730493c2a7c2b2a573050093958fa3730673079973089c73097e9a730a9d99a3730b730c0599c1a7c1b2a5730d00938cc7b2a5730e0001a390c1a7730f"
      )
      val ergoTreeT8Hex = ErgoTreeT8Hex.fromStringUnsafe(
        "d19683030191a38cc7a7019683020193c2b2a57300007473017302830108cdeeac93a38cc7b2a573030001978302019683040193b1a5730493c2a7c2b2a573050093958fa3730673079973089c73097e9a730a9d99a3730b730c0599c1a7c1b2a5730d00938cc7b2a5730e0001a390c1a7730f"
      )

    }

    object Foundation {
      val tx                     = TxId("e179f12156061c04d375f599bd8aea7ea5e704fab2d95300efb2d87460d60b83")
      val initialNanoErgs: Value = (4330791.5d * CoinsInOneErgo).toLong
      val inputBox: BoxId             = BoxId("5527430474b673e4aafb08e0079c639de23e6a17e87edd00f78662b43c88aeda")

      val address: Address = Address.fromStringUnsafe(
        "4L1ktFSzm3SH1UioDuUf5hyaraHird4D2dEACwQ1qHGjSKtA6KaNvSzRCZXZGf9jkfNAEC1SrYaZmCuvb2BKiXk5zW9xuvrXFT7FdNe2KqbymiZvo5UQLAm5jQY8ZBRhTZ4AFtZa1UF5nd4aofwPiL7YkJuyiL5hDHMZL1ZnyL746tHmRYMjAhCgE7d698dRhkdSeVy"
      )
      val ergoTree: ErgoTreeHex = ErgoTreeHex.fromStringUnsafe(
        "100e040004c094400580809cde91e7b0010580acc7f03704be944004808948058080c7b7e4992c0580b4c4c32104fe884804c0fd4f0580bcc1960b04befd4f05000400ea03d192c1b2a5730000958fa373019a73029c73037e997304a305958fa373059a73069c73077e997308a305958fa373099c730a7e99730ba305730cd193c2a7c2b2a5730d00d5040800"
      )
      val ergoTreeT8Hex = ErgoTreeT8Hex.fromStringUnsafe(
        "ea03d192c1b2a5730000958fa373019a73029c73037e997304a305958fa373059a73069c73077e997308a305958fa373099c730a7e99730ba305730cd193c2a7c2b2a5730d00d5040800"
      )
    }

    object NoPremine {
      val initialNanoErgs: Value = 1 * CoinsInOneErgo
      val box: BoxId             = BoxId("b8ce8cfe331e5eadfb0783bdc375c94413433f65e1e45857d71550d42e4d83bd")
      val address: Address       = Address.fromStringUnsafe("4MQyMKvMbnCJG3aJ")
      val ergoTree: ErgoTreeHex  = ErgoTreeHex.fromStringUnsafe("10010100d17300")
      val ergoTreeT8Hex          = ErgoTreeT8Hex.fromStringUnsafe("d17300")
    }

    object FeeContract {

      val address = Address.fromStringUnsafe(
        "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe"
      )

      val ergoTree: ErgoTreeHex = ErgoTreeParser.ergoTreeToHex(ErgoScriptPredef.feeProposition(MinerRewardDelta))
      val ergoTreeT8Hex =
        ErgoTreeT8Hex.fromStringUnsafe("d19683030193a38cc7b2a57300000193c2b2a57301007473027303830108cdeeac93b1a57304")
    }
  }

  val EpochLength = 1024

  val MinerRewardDelta = 720

  val TeamTreasuryThreshold = 67500000000L

  val CoinsInOneErgo: Long = 1000000000L
  val NanoOrder            = 1000000000d

  val Eip27UpperPoint        = 15 * CoinsInOneErgo
  val Eip27DefaultReEmission = 12 * CoinsInOneErgo
  val Eip27LowerPoint        = 3 * CoinsInOneErgo
  val Eip27ResidualEmission  = 3 * CoinsInOneErgo

  val MainnetEip27ActivationHeight = 777217
  val TestnetEip27ActivationHeight = 188001

  type NetworkPrefix = Byte
  type AddressPrefix = Byte

  val MainnetNetworkPrefix          = 0: NetworkPrefix
  val P2PKAddressTypePrefix: Byte   = 1: AddressPrefix
  val Pay2SHAddressTypePrefix: Byte = 2: AddressPrefix
  val Pay2SAddressTypePrefix: Byte  = 3: AddressPrefix

  def getAddressType(address: String): Try[AddressPrefix] =
    Base58.decode(address).map { bytes =>
      val headByte = bytes.head
      val t        = (headByte - MainnetNetworkPrefix).toByte
      require(
        t == P2PKAddressTypePrefix || t == Pay2SHAddressTypePrefix || t == Pay2SAddressTypePrefix,
        s"Unsupported address $address with type $t"
      )
      t
    }

}
