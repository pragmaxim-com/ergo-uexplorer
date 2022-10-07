package org.ergoplatform.uexplorer.db

import cats.syntax.traverse._
import io.circe.syntax._
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.node.{ApiFullBlock, RegisterValue}
import org.ergoplatform.uexplorer._

import scala.util.Try

/** Flattened representation of a full block from
  * Ergo protocol enriched with statistics.
  */
final case class FlatBlock(
  header: Header,
  info: BlockStats,
  extension: BlockExtension,
  adProofOpt: Option[AdProof],
  txs: List[Transaction],
  inputs: List[Input],
  dataInputs: List[DataInput],
  outputs: List[Output],
  assets: List[Asset],
  registers: List[BoxRegister],
  tokens: List[Token]
)

object FlatBlock {

  def apply(apiBlock: ApiFullBlock, prevBlockInfo: Option[BlockStats])(implicit
    protocolSettings: ProtocolSettings
  ): Try[FlatBlock] = {
    val apiHeader       = apiBlock.header
    val apiExtension    = apiBlock.extension
    val apiAdProofOpt   = apiBlock.adProofs
    val apiTransactions = apiBlock.transactions
    val header =
      Header(
        apiHeader.id,
        apiHeader.parentId,
        apiHeader.version,
        apiHeader.height,
        apiHeader.nBits,
        apiHeader.difficulty.value,
        apiHeader.timestamp,
        apiHeader.stateRoot,
        apiHeader.adProofsRoot,
        apiHeader.transactionsRoot,
        apiHeader.extensionHash,
        apiHeader.minerPk,
        apiHeader.w,
        apiHeader.n,
        apiHeader.d,
        apiHeader.votes,
        mainChain = false
      )

    val infoTry = BlockStats(apiBlock, prevBlockInfo)

    val extension =
      BlockExtension(
        apiExtension.headerId,
        apiExtension.digest,
        apiExtension.fields
      )

    val adProof =
      apiAdProofOpt.map { apiAdProof =>
        AdProof(
          apiAdProof.headerId,
          apiAdProof.proofBytes,
          apiAdProof.digest
        )
      }

    val txs: List[Transaction] = {
      val lastTxGlobalIndex = prevBlockInfo.map(_.maxTxGix).getOrElse(-1L)
      val headerId          = apiBlock.header.id
      val height            = apiBlock.header.height
      val ts                = apiBlock.header.timestamp
      val txs =
        apiTransactions.transactions.zipWithIndex
          .map { case (tx, i) =>
            val globalIndex = lastTxGlobalIndex + i + 1
            Transaction(tx.id, headerId, height, isCoinbase = false, ts, tx.size, i, globalIndex, mainChain = false)
          }
      val (init, coinbase) = txs.init -> txs.lastOption
      init ++ coinbase.map(_.copy(isCoinbase = true))
    }
    val inputs =
      apiTransactions.transactions.flatMap { apiTx =>
        apiTx.inputs.toList.zipWithIndex.map { case (i, index) =>
          Input(
            i.boxId,
            apiTx.id,
            apiTransactions.headerId,
            i.spendingProof.proofBytes,
            i.spendingProof.extension,
            index,
            mainChain = false
          )
        }
      }
    val dataInputs =
      apiTransactions.transactions.flatMap { apiTx =>
        apiTx.dataInputs.zipWithIndex.map { case (i, index) =>
          DataInput(
            i.boxId,
            apiTx.id,
            apiTransactions.headerId,
            index,
            mainChain = false
          )
        }
      }

    val outputsTry = {
      val lastOutputGlobalIndex          = prevBlockInfo.map(_.maxBoxGix).getOrElse(-1L)
      implicit val e: ErgoAddressEncoder = protocolSettings.addressEncoder
      apiTransactions.transactions.zipWithIndex
        .flatMap { case (tx, tix) =>
          tx.outputs.toList.zipWithIndex
            .map { case (o, oix) => ((o, tx.id), oix, tix) }
        }
        .sortBy { case (_, oix, tix) => (tix, oix) }
        .map { case ((o, txId), oix, _) => (o, oix, txId) }
        .zipWithIndex
        .traverse { case ((o, outIndex, txId), blockIndex) =>
          for {
            address            <- sigma.ergoTreeToAddress(o.ergoTree).map(t => Address.fromStringUnsafe(t.toString))
            scriptTemplateHash <- sigma.deriveErgoTreeTemplateHash(o.ergoTree)
            registersJson = RegistersParser.expand(o.additionalRegisters).asJson
            globalIndex   = lastOutputGlobalIndex + blockIndex + 1
          } yield Output(
            o.boxId,
            txId,
            apiTransactions.headerId,
            o.value,
            o.creationHeight,
            header.height,
            outIndex,
            globalIndex,
            o.ergoTree,
            scriptTemplateHash,
            address,
            registersJson,
            header.timestamp,
            mainChain = false
          )
        }
    }

    val assets =
      for {
        tx             <- apiTransactions.transactions
        out            <- tx.outputs.toList
        (asset, index) <- out.assets.zipWithIndex
      } yield Asset(asset.tokenId, out.boxId, apiTransactions.headerId, index, asset.amount)

    val registers =
      for {
        tx                            <- apiTransactions.transactions
        out                           <- tx.outputs.toList
        (id, rawValue)                <- out.additionalRegisters.toList
        RegisterValue(typeSig, value) <- RegistersParser.parseAny(rawValue).toOption
      } yield BoxRegister(id, out.boxId, typeSig, rawValue, value)

    val tokens =
      apiTransactions.transactions.flatMap { tx =>
        val allowedTokenId = TokenId.fromStringUnsafe(tx.inputs.head.boxId.value)
        for {
          out <- tx.outputs.toList.find(_.assets.map(_.tokenId).contains(allowedTokenId))
          props  = TokenPropsParser.parse(out.additionalRegisters)
          assets = out.assets.filter(_.tokenId == allowedTokenId)
          headAsset <- assets.headOption
          assetAmount = assets.map(_.amount).sum
        } yield Token(
          headAsset.tokenId,
          out.boxId,
          assetAmount,
          props.map(_.name),
          props.map(_.description),
          props.map(_ => TokenType.Eip004),
          props.map(_.decimals)
        )
      }

    for {
      info    <- infoTry
      outputs <- outputsTry
    } yield new FlatBlock(header, info, extension, adProof, txs, inputs, dataInputs, outputs, assets, registers, tokens)
  }
}
