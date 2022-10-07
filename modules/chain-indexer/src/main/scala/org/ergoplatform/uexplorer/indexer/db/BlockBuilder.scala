package org.ergoplatform.uexplorer.indexer.db

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.db._
import org.ergoplatform.uexplorer.indexer.progress.ProgressState.CachedBlock
import org.ergoplatform.uexplorer.node.{ApiFullBlock, RegisterValue}
import org.ergoplatform.uexplorer.{Address, TokenId, TokenType}
import io.circe.generic.auto._
import cats.syntax.traverse._
import io.circe.syntax._
import org.ergoplatform.uexplorer.indexer.ProtocolSettings
import org.ergoplatform.uexplorer.indexer.parser.{ErgoTreeParser, RegistersParser, TokenPropsParser}

import scala.util.Try

object BlockBuilder {

  def apply(apiBlock: ApiFullBlock, prevBlock: Option[CachedBlock])(implicit
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

    val txs: List[Transaction] = {
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
      val lastOutputGlobalIndex          = prevBlock.map(_.info.maxBoxGix).getOrElse(-1L)
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
            address            <- ErgoTreeParser.ergoTreeToAddress(o.ergoTree).map(t => Address.fromStringUnsafe(t.toString))
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

    def updateTotalInfo(
      currentBlockInfo: BlockInfo,
      currentTimestamp: Long,
      currentHeight: Int,
      prevBlock: CachedBlock
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
      import monocle.macros.syntax.lens._
      block
        .lens(_.header.mainChain)
        .modify(_ => mainChain)
        .lens(_.info.mainChain)
        .modify(_ => mainChain)
        .lens(_.txs)
        .modify(_.map(_.copy(mainChain = mainChain)))
        .lens(_.inputs)
        .modify(_.map(_.copy(mainChain = mainChain)))
        .lens(_.dataInputs)
        .modify(_.map(_.copy(mainChain = mainChain)))
        .lens(_.outputs)
        .modify(_.map(_.copy(mainChain = mainChain)))
        .lens(_.info)
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
