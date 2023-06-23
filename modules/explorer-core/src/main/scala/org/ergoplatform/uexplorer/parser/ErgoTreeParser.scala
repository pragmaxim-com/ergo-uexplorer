package org.ergoplatform.uexplorer.parser

import com.google.bitcoin.core.Base58
import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.{Address, ErgoTreeHex, ErgoTreeT8Hex}
import org.ergoplatform.{ErgoAddress, ErgoAddressEncoder, P2PKAddress, Pay2SAddress, Pay2SHAddress}
import scorex.crypto.hash.{Blake2b256, Digest32, Sha256}
import scorex.util.encode.Base16
import sigmastate.Values.{ErgoTree, FalseLeaf, SigmaPropConstant, Value}
import sigmastate.*
import sigmastate.serialization.{ErgoTreeSerializer, SigmaSerializer}

import scala.util.{Failure, Success, Try}
import eu.timepit.refined.auto.*
import io.circe.{Decoder, DecodingFailure}
import eu.timepit.refined.auto.*
import sigmastate.basics.DLogProtocol.ProveDlogProp

object ErgoTreeParser extends LazyLogging {

  private val treeSerializer: ErgoTreeSerializer = ErgoTreeSerializer.DefaultSerializer

  @inline def ergoTreeHex2ErgoTree(ergoTree: ErgoTreeHex): Try[Values.ErgoTree] =
    Base16.decode(ergoTree).map(treeSerializer.deserializeErgoTree)

  @inline def isErgoTreeT8(ergoTreeBytes: Array[Byte]): Boolean =
    treeSerializer.deserializeHeaderWithTreeBytes(SigmaSerializer.startReader(ergoTreeBytes))._3.nonEmpty

  @inline def ergoTreeHex2T8Hex(
    ergoTree: ErgoTreeHex
  )(implicit enc: ErgoAddressEncoder): Try[Option[ErgoTreeT8Hex]] =
    Base16.decode(ergoTree).map {
      case bytes if isErgoTreeT8(bytes) =>
        val tree = treeSerializer.deserializeErgoTree(bytes)
        val t8Opt =
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
        t8Opt.map(t => ErgoTreeT8Hex.fromStringUnsafe(Base16.encode(t)))
      case _ =>
        Option.empty[ErgoTreeT8Hex]
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

  @inline def base58Address2ErgoTreeT8Hex(
    address: Address
  )(implicit enc: ErgoAddressEncoder): Try[ErgoTreeT8Hex] =
    base58Address2ErgoTree(address).map { ergoTree =>
      ErgoTreeT8Hex.fromStringUnsafe(Base16.encode(ergoTree.template))
    }

  @inline def base58Address2ErgoTree(address: Address)(implicit enc: ErgoAddressEncoder): Try[ErgoTree] =
    base58AddressToErgoAddress(address).map(_.script)

  @inline def ergoTreeHex2Base58Address(ergoTreeHex: ErgoTreeHex)(implicit enc: ErgoAddressEncoder): Try[Address] =
    ergoAddress2Base58Address(ergoTreeHex2ErgoAddress(ergoTreeHex))

  def base58AddressToErgoAddress(address: Address)(implicit enc: ErgoAddressEncoder): Try[ErgoAddress] =
    enc.fromString(address)

  def ergoTreeToHex(ergoTree: ErgoTree): ErgoTreeHex = ErgoTreeHex.fromStringUnsafe(Base16.encode(ergoTree.bytes))
}
