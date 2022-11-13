package org.ergoplatform.uexplorer.indexer.parser

import org.ergoplatform.uexplorer.{ErgoTreeTemplateHash, HexString}
import org.ergoplatform.{ErgoAddress, ErgoAddressEncoder, Pay2SAddress}
import scorex.crypto.hash.Sha256
import scorex.util.encode.Base16
import sigmastate.Values.FalseLeaf
import sigmastate.*
import sigmastate.serialization.ErgoTreeSerializer

import scala.util.{Failure, Success, Try}
import eu.timepit.refined.auto.*

object ErgoTreeParser {

  private val treeSerializer: ErgoTreeSerializer = ErgoTreeSerializer.DefaultSerializer

  @inline def deserializeErgoTree(raw: HexString): Try[Values.ErgoTree] =
    Base16.decode(raw).map(treeSerializer.deserializeErgoTree)

  @inline def deriveErgoTreeTemplateHash(ergoTree: HexString): Try[ErgoTreeTemplateHash] =
    deserializeErgoTree(ergoTree).map { tree =>
      ErgoTreeTemplateHash.fromStringUnsafe(Base16.encode(Sha256.hash(tree.template)))
    }

  @inline def ergoTreeToAddress(
    ergoTree: HexString
  )(implicit enc: ErgoAddressEncoder): Try[ErgoAddress] =
    Base16
      .decode(ergoTree)
      .flatMap { bytes =>
        enc.fromProposition(treeSerializer.deserializeErgoTree(bytes))
      }
      .recover(_ => Pay2SAddress(FalseLeaf.toSigmaProp): ErgoAddress)
}
