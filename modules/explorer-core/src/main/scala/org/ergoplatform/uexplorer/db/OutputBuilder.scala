package org.ergoplatform.uexplorer.db

import org.ergoplatform.{ErgoAddressEncoder, ErgoScriptPredef, Pay2SAddress}
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ExpandedRegister}
import org.ergoplatform.uexplorer.parser.{ErgoTreeParser, RegistersParser}
import scorex.util.encode.Base16
import sigmastate.basics.DLogProtocol.ProveDlog
import sigmastate.serialization.{GroupElementSerializer, SigmaSerializer}

import scala.collection.immutable.ArraySeq
import scala.util.Try

object OutputBuilder {

  private def getOutputRecords(block: BlockWithReward)(implicit enc: ErgoAddressEncoder): Try[ArraySeq[OutputRecord]] = Try {
    block.b.transactions.transactions.flatMap { tx =>
      tx.outputs.map { o =>
        (
          for {
            address            <- ErgoTreeParser.ergoTreeHex2Base58Address(o.ergoTree)
            scriptTemplateHash <- ErgoTreeParser.ergoTreeHex2ErgoTreeTemplateSha256Hex(o.ergoTree)
            additionalRegisters = o.additionalRegisters.view.mapValues(hex => RegistersParser.parseAny(hex)).toMap
          } yield OutputRecord(tx.id, o.boxId, o.ergoTree, scriptTemplateHash, address, o.value, additionalRegisters)
        ).get
      }
    }
  }

  def apply(block: BlockWithReward)(implicit enc: ErgoAddressEncoder): Try[BlockWithOutputs] =
    getOutputRecords(block).map(outputRecords => block.toBlockWithOutput(outputRecords))
}
