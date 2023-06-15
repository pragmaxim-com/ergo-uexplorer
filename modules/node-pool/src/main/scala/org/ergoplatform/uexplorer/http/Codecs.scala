package org.ergoplatform.uexplorer.http

import io.circe.{Decoder, HCursor}
import org.ergoplatform.uexplorer.BoxId
import org.ergoplatform.uexplorer.node.*
import io.circe.generic.auto.*
import org.ergoplatform.ErgoAddressEncoder
import eu.timepit.refined.api.RefType.tagRefType.unsafeWrap
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto.*
import eu.timepit.refined.refineV
import eu.timepit.refined.string.{HexStringSpec, MatchesRegex}
import io.circe.parser.*
import io.circe.refined.*
import io.circe.syntax.*
import io.circe.*
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.*

import scala.collection.immutable.ArraySeq
import scala.util.{Failure, Success, Try}
import org.ergoplatform.uexplorer.parser.ErgoTreeParser
import org.ergoplatform.uexplorer.parser.RegistersParser

trait Codecs {

  implicit val apiDataInputDecoder: Decoder[ApiDataInput] =
    _.downField("boxId").as[BoxId].map(ApiDataInput(_))

  implicit val apiDifficultyDecoder: Decoder[ApiDifficulty] =
    Decoder.decodeString.emapTry { str =>
      Try {
        val bInt = BigDecimal(str)
        ApiDifficulty(bInt)
      }
    }

  implicit val apiHeaderDecoder: Decoder[ApiHeader] = { (c: HCursor) =>
    for {
      id               <- c.downField("id").as[BlockId]
      parentId         <- c.downField("parentId").as[BlockId]
      version          <- c.downField("version").as[Byte]
      height           <- c.downField("height").as[Height]
      nBits            <- c.downField("nBits").as[Long]
      difficulty       <- c.downField("difficulty").as[ApiDifficulty]
      timestamp        <- c.downField("timestamp").as[Long]
      stateRoot        <- c.downField("stateRoot").as[HexString]
      adProofsRoot     <- c.downField("adProofsRoot").as[HexString]
      transactionsRoot <- c.downField("transactionsRoot").as[HexString]
      extensionHash    <- c.downField("extensionHash").as[HexString]
      powSolutions     <- c.downField("powSolutions").as[ApiPowSolutions]
      votes            <- c.downField("votes").as[String]
    } yield ApiHeader(
      id,
      parentId,
      version,
      height,
      nBits,
      difficulty,
      timestamp,
      stateRoot,
      adProofsRoot,
      transactionsRoot,
      extensionHash,
      powSolutions.pk,
      powSolutions.w,
      powSolutions.n,
      powSolutions.d,
      votes
    )
  }

  implicit def apiOutputDecoder(implicit enc: ErgoAddressEncoder): Decoder[ApiOutput] = { (c: HCursor) =>
    for {
      boxId               <- c.downField("boxId").as[BoxId]
      value               <- c.downField("value").as[Value]
      creationHeight      <- c.downField("creationHeight").as[Height]
      ergoTree            <- c.downField("ergoTree").as[HexString]
      address             <- Right(ErgoTreeParser.ergoTreeToAddress(ergoTree))
      scriptTemplateHash  <- ErgoTreeParser.deriveErgoTreeTemplateHash(ergoTree)
      assets              <- c.downField("assets").as[List[ApiAsset]]
      additionalRegisters <- c.downField("additionalRegisters").as[Map[RegisterId, HexString]]
    } yield ApiOutput(
      boxId,
      value,
      creationHeight,
      ergoTree,
      address,
      scriptTemplateHash,
      assets,
      additionalRegisters.view.mapValues(hex => RegistersParser.parseAny(hex)).toMap
    )
  }

  implicit val apiPowSolutionsDecoder: Decoder[ApiPowSolutions] = { (c: HCursor) =>
    for {
      pk <- c.downField("pk").as[HexString]
      w  <- c.downField("w").as[HexString]
      n  <- c.downField("n").as[HexString]
      d  <- c.downField("d").as[BigInt]
    } yield ApiPowSolutions(pk, w, n, d.toString())
  }

  implicit val apiSpendingProofDecoder: Decoder[ApiSpendingProof] = { (c: HCursor) =>
    for {
      proofBytes <- c.downField("proofBytes").as[String].flatMap { s =>
                      Try(HexString.fromStringUnsafe(s)) match {
                        case Failure(_)     => Right[DecodingFailure, Option[HexString]](Option.empty)
                        case Success(value) => Right[DecodingFailure, Option[HexString]](Option(value))
                      }
                    }
      extension <- c.downField("extension").as[Json]
    } yield ApiSpendingProof(proofBytes, extension)
  }

  implicit def apiTransactionDecoder(implicit enc: ErgoAddressEncoder): Decoder[ApiTransaction] = { (c: HCursor) =>
    for {
      id         <- c.downField("id").as[TxId]
      inputs     <- c.downField("inputs").as[ArraySeq[ApiInput]]
      dataInputs <- c.downField("dataInputs").as[List[ApiDataInput]]
      outputs    <- c.downField("outputs").as[ArraySeq[ApiOutput]]
      size       <- c.downField("size").as[Int]
    } yield ApiTransaction(id, inputs, dataInputs, outputs, size)
  }

  // extremely CPU greedy (6% of all runtime)
  implicit def apiFullBlockDecoder(implicit enc: ErgoAddressEncoder): Decoder[ApiFullBlock] = { (c: HCursor) =>
    for {
      header       <- c.downField("header").as[ApiHeader]
      transactions <- c.downField("blockTransactions").as[ApiBlockTransactions]
      extension    <- c.downField("extension").as[ApiBlockExtension]
      adProofs <- c.downField("adProofs").as[ApiAdProof] match {
                    case Left(_)       => Right(None)
                    case Right(proofs) => Right(Some(proofs))
                  }
      size <- c.downField("size").as[Int]
    } yield ApiFullBlock(header, transactions, extension, adProofs, size)
  }

}
