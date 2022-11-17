package org.ergoplatform.uexplorer.indexer.db

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.indexer.progress.ProgressState.CachedBlockInfo
import org.ergoplatform.uexplorer.node.{ApiFullBlock, ExpandedRegister, RegisterValue}
import org.ergoplatform.uexplorer.{Address, HexString, SigmaType, TokenId, TokenType}
import io.circe.generic.auto.*
import io.circe.refined.*
import cats.syntax.traverse.*
import io.circe.syntax.*
import org.ergoplatform.uexplorer.indexer.config.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.parser.{ErgoTreeParser, RegistersParser, TokenPropsParser}
import eu.timepit.refined.auto.*
import io.circe.Encoder

import scala.collection.immutable.ArraySeq
import scala.util.Try

object BlockBuilder {

  def apply(apiBlock: ApiFullBlock, prevBlock: Option[CachedBlockInfo])(implicit
                                                                        protocolSettings: ProtocolSettings
  ): Try[Block] = {
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

    val infoTry = BlockInfoBuilder(apiBlock, prevBlock)

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

    val txs: ArraySeq[Transaction] = {
      val lastTxGlobalIndex = prevBlock.map(_.info.maxTxGix).getOrElse(-1L)
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
        apiTx.inputs.zipWithIndex.map { case (i, index) =>
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
      summon[Encoder[HexString]]
      summon[Encoder[ExpandedRegister]]
      val lastOutputGlobalIndex          = prevBlock.map(_.info.maxBoxGix).getOrElse(-1L)
      implicit val e: ErgoAddressEncoder = protocolSettings.addressEncoder
      apiTransactions.transactions.zipWithIndex
        .flatMap { case (tx, tix) =>
          tx.outputs.zipWithIndex
            .map { case (o, oix) => ((o, tx.id), oix, tix) }
        }
        .sortBy { case (_, oix, tix) => (tix, oix) }
        .map { case ((o, txId), oix, _) => (o, oix, txId) }
        .zipWithIndex
        .traverse { case ((o, outIndex, txId), blockIndex) =>
          for {
            address            <- ErgoTreeParser.ergoTreeToAddress(o.ergoTree)
            scriptTemplateHash <- ErgoTreeParser.deriveErgoTreeTemplateHash(o.ergoTree)
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
        val allowedTokenId = TokenId.fromStringUnsafe(tx.inputs.head.boxId.unwrapped)
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

    def updateTotalInfo(
      currentBlockInfo: BlockInfo,
      currentTimestamp: Long,
      currentHeight: Int,
      prevBlock: CachedBlockInfo
    ): BlockInfo =
      currentBlockInfo.copy(
        blockChainTotalSize = prevBlock.info.blockChainTotalSize + currentBlockInfo.blockSize,
        totalTxsCount       = prevBlock.info.totalTxsCount + currentBlockInfo.txsCount,
        totalCoinsIssued    = protocolSettings.emission.issuedCoinsAfterHeight(currentHeight),
        totalMiningTime     = prevBlock.info.totalMiningTime + (currentTimestamp - prevBlock.timestamp),
        totalFees           = prevBlock.info.totalFees + currentBlockInfo.blockFee,
        totalMinersReward   = prevBlock.info.totalMinersReward + currentBlockInfo.minerReward,
        totalCoinsInTxs     = prevBlock.info.totalCoinsInTxs + currentBlockInfo.blockCoins
      )

    def updateMainChain(block: Block, mainChain: Boolean): Block = {
      import monocle.syntax.all._
      block
        .focus(_.header.mainChain)
        .modify(_ => mainChain)
        .focus(_.txs)
        .modify(_.map(_.copy(mainChain = mainChain)))
        .focus(_.inputs)
        .modify(_.map(_.copy(mainChain = mainChain)))
        .focus(_.dataInputs)
        .modify(_.map(_.copy(mainChain = mainChain)))
        .focus(_.outputs)
        .modify(_.map(_.copy(mainChain = mainChain)))
        .focus(_.info)
        .modify {
          case currentBlockInfo if prevBlock.nonEmpty =>
            updateTotalInfo(currentBlockInfo, block.header.timestamp, block.header.height, prevBlock.get)
          case currentBlockInfo =>
            currentBlockInfo
        }
    }

    for {
      info    <- infoTry
      outputs <- outputsTry
    } yield updateMainChain(
      Block(header, extension, adProof, txs, inputs, dataInputs, outputs, assets, registers, tokens, info),
      mainChain = true
    )
  }
}
