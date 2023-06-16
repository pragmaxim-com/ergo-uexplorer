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

  @inline def deserializeErgoTree(raw: HexString): Try[Values.ErgoTree] =
    Base16.decode(raw).map(treeSerializer.deserializeErgoTree)

  @inline def deriveErgoTreeTemplateHash(ergoTree: HexString): Decoder.Result[ErgoTreeTemplateHash] =
    deserializeErgoTree(ergoTree)
      .map { tree =>
        ErgoTreeTemplateHash.fromStringUnsafe(Base16.encode(Sha256.hash(tree.template)))
      } match {
      case Success(t)  => Right(t)
      case Failure(ex) => Left(DecodingFailure.fromThrowable(ex, List.empty))
    }

  private def addressToString(address: ErgoAddress)(implicit enc: ErgoAddressEncoder): String = {
    val withNetworkByte = (enc.networkPrefix + address.addressTypePrefix).toByte +: address.contentBytes

    val checksum = ErgoAddressEncoder.hash256(withNetworkByte).take(ErgoAddressEncoder.ChecksumLength)
    Base58.encode(withNetworkByte ++ checksum)
  }

  @inline def ergoTreeToAddress(
    ergoTree: HexString
  )(implicit enc: ErgoAddressEncoder): Try[Address] = Try {
    Address.fromStringUnsafe(
      addressToString(
        Base16
          .decode(ergoTree)
          .flatMap { bytes =>
            enc.fromProposition(treeSerializer.deserializeErgoTree(bytes))
          }
          .getOrElse(Pay2SAddress(FalseLeaf.toSigmaProp): ErgoAddress)
      )
    )
  }
}
