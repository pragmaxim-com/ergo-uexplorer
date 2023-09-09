package org.ergoplatform.uexplorer.db

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.RegisterId.*
import org.ergoplatform.uexplorer.parser.{ErgoTreeParser, RegistersParser}
import zio.{Task, ZIO}

import scala.collection.mutable

object OutputBuilder {

  private def getOutputRecords(block: BlockWithReward)(implicit enc: ErgoAddressEncoder): Task[OutputRecords] =
    ZIO.attempt {
      val byErgoTree     = mutable.Map.empty[ErgoTree, mutable.Set[Utxo]]
      val byErgoTreeT8   = mutable.Map.empty[ErgoTreeT8, mutable.Set[Utxo]]
      val utxosByTokenId = mutable.Map.empty[TokenId, mutable.Set[BoxId]]
      val tokensByUtxo   = mutable.Map.empty[BoxId, mutable.Map[TokenId, Amount]]

      block.b.transactions.transactions.foreach { tx =>
        tx.outputs.foreach { o =>
          val (ergoTreeHash, ergoTreeT8Opt) = ErgoTreeParser.ergoTreeHex2T8(o.ergoTree).get
          val additionalRegisters           = o.additionalRegisters.view.mapValues(hex => RegistersParser.parseAny(hex).serializedValue).toMap
          o.assets.foreach { asset =>
            adjustMultiSet(utxosByTokenId, asset.tokenId, o.boxId)
            adjustMultiMap(tokensByUtxo, o.boxId, asset.tokenId, asset.amount)
          }
          val utxo =
            Utxo(
              o.boxId,
              tx.id,
              o.creationHeight,
              block.b.header.height,
              ergoTreeHash,
              ergoTreeT8Opt.map(_._2),
              o.value,
              additionalRegisters.get(R4),
              additionalRegisters.get(R5),
              additionalRegisters.get(R6),
              additionalRegisters.get(R7),
              additionalRegisters.get(R8),
              additionalRegisters.get(R9)
            )
          adjustMultiSet(byErgoTree, ErgoTree(ergoTreeHash, block.b.header.id, o.ergoTree), utxo)
          ergoTreeT8Opt.foreach { case (ergoTreeT8Hex, ergoTreeT8Hash) =>
            adjustMultiSet(byErgoTreeT8, ErgoTreeT8(ergoTreeT8Hash, block.b.header.id, ergoTreeT8Hex), utxo)
          }
        }
      }
      OutputRecords(byErgoTree, byErgoTreeT8, utxosByTokenId, tokensByUtxo)
    }

  private def adjustMultiSet[ET, K](m: mutable.Map[ET, mutable.Set[K]], et: ET, k: K) =
    m.adjust(et)(_.fold(mutable.Set(k))(_.addOne(k)))

  private def adjustMultiMap[ET, K, V](m: mutable.Map[ET, mutable.Map[K, V]], et: ET, k: K, v: V) =
    m.adjust(et)(_.fold(mutable.Map(k -> v))(_.addOne(k -> v)))

  def apply(block: BlockWithReward)(implicit enc: ErgoAddressEncoder): Task[BlockWithOutputs] =
    getOutputRecords(block).map(outputRecords => block.toBlockWithOutput(outputRecords))
}
