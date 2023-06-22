package org.ergoplatform.uexplorer.parser

import com.google.bitcoin.core.Base58
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.{Address, ErgoTreeHex, TemplateHashHex}
import org.ergoplatform.{ErgoAddress, ErgoAddressEncoder, P2PKAddress, Pay2SAddress, Pay2SHAddress}
import scorex.crypto.hash.{Blake2b256, Digest32, Sha256}
import scorex.util.encode.Base16
import sigmastate.Values.{ErgoTree, FalseLeaf, SigmaPropConstant, Value}
import sigmastate.*
import sigmastate.serialization.ErgoTreeSerializer

import scala.util.{Failure, Success, Try}
import eu.timepit.refined.auto.*
import io.circe.{Decoder, DecodingFailure}
import eu.timepit.refined.auto.*
import sigmastate.basics.DLogProtocol.ProveDlogProp

object ErgoTreeParser extends LazyLogging {

  private val treeSerializer: ErgoTreeSerializer = ErgoTreeSerializer.DefaultSerializer

  @inline def ergoTreeHex2ErgoTree(ergoTree: ErgoTreeHex): Try[Values.ErgoTree] =
    Base16.decode(ergoTree).map(treeSerializer.deserializeErgoTree)

  @inline def ergoTreeHex2ErgoTreeTemplateSha256Hex(
    ergoTree: ErgoTreeHex
  )(implicit enc: ErgoAddressEncoder): Try[Option[TemplateHashHex]] =
    ergoTreeHex2ErgoTree(ergoTree)
      .map { tree =>
        val templateOpt =
          tree.root match {
            case Right(SigmaPropConstant(ProveDlogProp(_))) =>
              None
            case Right(enc.IsPay2SHAddress(_)) =>
              Option(tree.template)
            case Right(b: Value[SSigmaProp.type] @unchecked) if b.tpe == SSigmaProp =>
              Option(tree.template)
            case _ =>
              None
          }
        templateOpt.map(t => TemplateHashHex.fromStringUnsafe(Base16.encode(Sha256.hash(t))))
      }

  // http://213.239.193.208:9053/blocks/at/545684
  // http://213.239.193.208:9053/blocks/2ad5af788bfd1b92790eadb42a300ad4fc38aaaba599a43574b1ea45d5d9dee4
  // http://213.239.193.208:9053/utils/ergoTreeToAddress/cd07021a8e6f59fd4a
  // Note that some ErgoTree can be invalid
  @inline def ergoTreeHex2ErgoAddress(ergoTreeHex: ErgoTreeHex)(implicit enc: ErgoAddressEncoder): ErgoAddress =
    ergoTreeHex2ErgoTree(ergoTreeHex)
      .flatMap(enc.fromProposition)
      .getOrElse(Pay2SAddress(FalseLeaf.toSigmaProp): ErgoAddress)

  @inline def ergoAddress2Base58Address(address: ErgoAddress)(implicit enc: ErgoAddressEncoder): Try[Address] = Try {
    val withNetworkByte = (enc.networkPrefix + address.addressTypePrefix).toByte +: address.contentBytes
    val checksum        = ErgoAddressEncoder.hash256(withNetworkByte).take(ErgoAddressEncoder.ChecksumLength)
    // avoiding Address.fromStringUnsafe, as Base58 produced valid result for all Ergo addresses so far
    Base58.encode(withNetworkByte ++ checksum).asInstanceOf[Address]
  }

  @inline def base58Address2ErgoTreeHex(address: Address)(implicit enc: ErgoAddressEncoder): Try[ErgoTreeHex] =
    base58Address2ErgoTree(address).map { ergoTree =>
      ErgoTreeHex.fromStringUnsafe(Base16.encode(ergoTree.bytes))
    }

  @inline def base58Address2ErgoTreeTemplateHex(address: Address)(implicit enc: ErgoAddressEncoder): Try[TemplateHashHex] =
    base58Address2ErgoTree(address).map { ergoTree =>
      TemplateHashHex.fromStringUnsafe(Base16.encode(ergoTree.template))
    }

  @inline def base58Address2ErgoTree(address: Address)(implicit enc: ErgoAddressEncoder): Try[ErgoTree] =
    base58AddressToErgoAddress(address).map(_.script)

  @inline def ergoTreeHex2Base58Address(ergoTreeHex: ErgoTreeHex)(implicit enc: ErgoAddressEncoder): Try[Address] =
    ergoAddress2Base58Address(ergoTreeHex2ErgoAddress(ergoTreeHex))

  def base58AddressToErgoAddress(address: Address)(implicit enc: ErgoAddressEncoder): Try[ErgoAddress] =
    enc.fromString(address)

  def ergoTreeToHex(ergoTree: ErgoTree): ErgoTreeHex = ErgoTreeHex.fromStringUnsafe(Base16.encode(ergoTree.bytes))
}
