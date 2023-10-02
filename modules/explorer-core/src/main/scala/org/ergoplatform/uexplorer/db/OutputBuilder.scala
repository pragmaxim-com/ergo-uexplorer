package org.ergoplatform.uexplorer.db

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.RegisterId.*
import org.ergoplatform.uexplorer.parser.{ErgoTreeParser, RegistersParser, TokenPropsParser}
import zio.{Task, ZIO}

import scala.collection.mutable

object OutputBuilder {

  private def getOutputRecords(block: BlockWithReward)(implicit enc: ErgoAddressEncoder): Task[OutputRecords] =
    ZIO.attempt {
      val byErgoTree     = mutable.Map.empty[ErgoTree, mutable.Set[Utxo]]
      val byErgoTreeT8   = mutable.Map.empty[ErgoTreeT8, mutable.Set[Utxo]]
      val utxosByTokenId = mutable.Map.empty[TokenId, mutable.Set[BoxId]]
      val tokensByUtxo   = mutable.Map.empty[BoxId, mutable.Map[TokenId, Token]]

      block.b.transactions.transactions.foreach { tx =>
        val allowedTokenId = TokenId.fromStringUnsafe(tx.inputs.head.boxId.unwrapped)
        tx.outputs.zipWithIndex.foreach { case (o, outputIndex) =>
          val additionalRegisters           = o.additionalRegisters.view.mapValues(RegistersParser.parseAny).toMap
          val (ergoTreeHash, ergoTreeT8Opt) = ErgoTreeParser.ergoTreeHex2T8(o.ergoTree).get
          o.assets.zipWithIndex.foreach {
            case (asset, assetIndex) if asset.tokenId == allowedTokenId =>
              val props = TokenPropsParser.parse(additionalRegisters)
              val token =
                Token(
                  assetIndex,
                  asset.amount,
                  props.map(_.name),
                  props.map(_.description),
                  props.map(_ => TokenType.Eip004),
                  props.map(_.decimals)
                )
              adjustMultiSet(utxosByTokenId, asset.tokenId, o.boxId)
              adjustMultiMap(tokensByUtxo, o.boxId, asset.tokenId, token)
            case (asset, index) =>
              val token =
                Token(
                  index,
                  asset.amount,
                  Option.empty,
                  Option.empty,
                  Option.empty,
                  Option.empty
                )
              adjustMultiSet(utxosByTokenId, asset.tokenId, o.boxId)
              adjustMultiMap(tokensByUtxo, o.boxId, asset.tokenId, token)
          }
          val utxo =
            Utxo(
              o.boxId,
              tx.id,
              block.b.header.id,
              o.creationHeight,
              block.b.header.height,
              ergoTreeHash,
              ergoTreeT8Opt.map(_._2),
              o.value,
              outputIndex,
              additionalRegisters.get(R4).map(_.serializedValue),
              additionalRegisters.get(R5).map(_.serializedValue),
              additionalRegisters.get(R6).map(_.serializedValue),
              additionalRegisters.get(R7).map(_.serializedValue),
              additionalRegisters.get(R8).map(_.serializedValue),
              additionalRegisters.get(R9).map(_.serializedValue)
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
