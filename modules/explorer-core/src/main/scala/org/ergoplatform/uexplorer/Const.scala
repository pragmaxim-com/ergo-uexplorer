package org.ergoplatform.uexplorer

import org.ergoplatform.uexplorer.{BoxId, HexString}
import org.ergoplatform.ErgoScriptPredef
import scorex.util.encode.{Base16, Base58}
import scala.util.Try

object Const {

  object Genesis {

    val blockId = BlockId.fromStringUnsafe("0000000000000000000000000000000000000000000000000000000000000000")
    
    object Emission {
      val tx                    = TxId("4c6282be413c6e300a530618b37790be5f286ded758accc2aebd41554a1be308")
      val initialNanoErgs: Long = 93409132 * CoinsInOneErgo
      val box: BoxId            = BoxId("b69575e11c5c43400bfead5976ee0d6245a1168396b2e2a4f384691f275d501c")

      val address: Address = Address.fromStringUnsafe(
        "2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU"
      )
    }

    object Foundation {
      val tx                    = TxId("e179f12156061c04d375f599bd8aea7ea5e704fab2d95300efb2d87460d60b83")
      val initialNanoErgs: Long = (4330791.5d * CoinsInOneErgo).toLong
      val box: BoxId            = BoxId("5527430474b673e4aafb08e0079c639de23e6a17e87edd00f78662b43c88aeda")

      val address: Address = Address.fromStringUnsafe(
        "4L1ktFSzm3SH1UioDuUf5hyaraHird4D2dEACwQ1qHGjSKtA6KaNvSzRCZXZGf9jkfNAEC1SrYaZmCuvb2BKiXk5zW9xuvrXFT7FdNe2KqbymiZvo5UQLAm5jQY8ZBRhTZ4AFtZa1UF5nd4aofwPiL7YkJuyiL5hDHMZL1ZnyL746tHmRYMjAhCgE7d698dRhkdSeVy"
      )
    }

    object NoPremine {
      val initialNanoErgs: Long = 1 * CoinsInOneErgo
      val box: BoxId            = BoxId("b8ce8cfe331e5eadfb0783bdc375c94413433f65e1e45857d71550d42e4d83bd")
      val address: Address      = Address.fromStringUnsafe("4MQyMKvMbnCJG3aJ")
    }
  }

  /** These form very big partitions in secondary indexes of node_outputs table which leads to garbage collecting related
    * crashes during indexing
    */
  object FeeContract {

    val address = Address.fromStringUnsafe(
      "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe"
    )

    val ergoTree =
      "1005040004000e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a701730073011001020402d19683030193a38cc7b2a57300000193c2b2a57301007473027303830108cdeeac93b1a57304"

    val ergoTreeTemplateHash =
      "5b710d70f207f03745a8bb713006f235446f0104f89e977ae066ae184c0494fa"
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

  val FeePropositionScriptHex: HexString = {
    val script = ErgoScriptPredef.feeProposition(MinerRewardDelta)
    HexString.fromStringUnsafe(Base16.encode(script.bytes))
  }

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
