package org.ergoplatform.uexplorer.indexer

import org.ergoplatform.uexplorer.Const.MinerRewardDelta
import org.ergoplatform.uexplorer.{Address, HexString}
import org.ergoplatform.ErgoScriptPredef
import scorex.util.encode.{Base16, Base58}

import scala.util.Try

object Const {

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
