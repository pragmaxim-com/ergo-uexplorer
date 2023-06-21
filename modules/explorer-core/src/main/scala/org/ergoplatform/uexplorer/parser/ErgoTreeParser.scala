package org.ergoplatform.uexplorer.parser

import com.google.bitcoin.core.Base58
import org.ergoplatform.uexplorer.{Address, ErgoTreeTemplateHash, HexString}
import org.ergoplatform.{ErgoAddress, ErgoAddressEncoder, Pay2SAddress}
import scorex.crypto.hash.Sha256
import scorex.util.encode.Base16
import sigmastate.Values.FalseLeaf
import sigmastate.*
import sigmastate.serialization.ErgoTreeSerializer

import scala.util.{Failure, Success, Try}
import eu.timepit.refined.auto.*
import io.circe.{Decoder, DecodingFailure}

object ErgoTreeParser {

  private val treeSerializer: ErgoTreeSerializer = ErgoTreeSerializer.DefaultSerializer

  @inline def deserializeErgoTree(ergoTree: HexString): Try[Values.ErgoTree] =
    Base16.decode(ergoTree).map(treeSerializer.deserializeErgoTree)

  @inline def deriveErgoTreeTemplateHash(ergoTree: HexString): Try[ErgoTreeTemplateHash] =
    deserializeErgoTree(ergoTree)
      .map { tree =>
        ErgoTreeTemplateHash.fromStringUnsafe(Base16.encode(Sha256.hash(tree.template)))
      }

  @inline def deserializeErgoTreeHexToAddress(ergoTreeHex: HexString)(implicit enc: ErgoAddressEncoder): Try[ErgoAddress] =
    deserializeErgoTree(ergoTreeHex).flatMap(enc.fromProposition)

  @inline def encodeErgoAddressToString(address: ErgoAddress)(implicit enc: ErgoAddressEncoder): Try[Address] = Try {
    val withNetworkByte = (enc.networkPrefix + address.addressTypePrefix).toByte +: address.contentBytes
    val checksum        = ErgoAddressEncoder.hash256(withNetworkByte).take(ErgoAddressEncoder.ChecksumLength)
    // avoiding Address.fromStringUnsafe, as Base58 produced valid result for all Ergo addresses so far
    Base58.encode(withNetworkByte ++ checksum).asInstanceOf[Address]
  }

  @inline def ergoTreeHexToAddressString(ergoTreeHex: HexString)(implicit enc: ErgoAddressEncoder): Try[Address] =
    deserializeErgoTreeHexToAddress(ergoTreeHex).flatMap(encodeErgoAddressToString)

  def ergoAddressFromString(address: Address)(implicit enc: ErgoAddressEncoder): Try[ErgoAddress] =
    enc.fromString(address)
}
