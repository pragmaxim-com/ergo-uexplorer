package org.ergoplatform.uexplorer.node

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
import org.ergoplatform.uexplorer.parser.{ErgoTreeParser, RegistersParser}

import scala.collection.immutable.ArraySeq
import scala.util.{Failure, Success, Try}

final case class ApiAdProof(
  headerId: BlockId,
  proofBytes: HexString,
  digest: HexString
)

final case class ApiAsset(
  tokenId: TokenId,
  amount: Long
)

final case class ApiBlockExtension(
  headerId: BlockId,
  digest: HexString,
  fields: Json
)

final case class ApiBlockTransactions(
  headerId: BlockId,
  transactions: ArraySeq[ApiTransaction]
)

final case class ApiDataInput(boxId: BoxId)

object ApiDataInput {
  implicit val decoder: Decoder[ApiDataInput] = _.downField("boxId").as[BoxId].map(ApiDataInput(_))
}

final case class ApiDifficulty(value: BigDecimal)

object ApiDifficulty {

  implicit val decoder: Decoder[ApiDifficulty] =
    Decoder.decodeString.emapTry { str =>
      Try {
        val bInt = BigDecimal(str)
        ApiDifficulty(bInt)
      }
    }
}

final case class ApiFullBlock(
  header: ApiHeader,
  transactions: ApiBlockTransactions,
  extension: ApiBlockExtension,
  adProofs: Option[ApiAdProof],
  size: Int
)

object ApiFullBlock {
  import io.circe.generic.auto.*

  implicit def decoder(implicit enc: ErgoAddressEncoder): Decoder[ApiFullBlock] = { (c: HCursor) =>
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

final case class ApiHeader(
  id: BlockId,
  parentId: BlockId,
  version: Byte,
  height: Int,
  nBits: Long,
  difficulty: ApiDifficulty,
  timestamp: Long,
  stateRoot: HexString,
  adProofsRoot: HexString,
  transactionsRoot: HexString,
  extensionHash: HexString,
  minerPk: HexString,
  w: HexString,
  n: HexString,
  d: String,
  votes: String
)

object ApiHeader {

  implicit val decoder: Decoder[ApiHeader] = { (c: HCursor) =>
    for {
      id               <- c.downField("id").as[BlockId]
      parentId         <- c.downField("parentId").as[BlockId]
      version          <- c.downField("version").as[Byte]
      height           <- c.downField("height").as[Int]
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
}

final case class ApiOutput(
  boxId: BoxId,
  value: Long,
  creationHeight: Int,
  ergoTree: HexString,
  address: Address,
  scriptTemplateHash: ErgoTreeTemplateHash,
  assets: List[ApiAsset],
  additionalRegisters: Map[RegisterId, ExpandedRegister]
)

object ApiOutput {
  import io.circe.generic.auto.*

  implicit def jsonDecoder(implicit enc: ErgoAddressEncoder): Decoder[ApiOutput] = { (c: HCursor) =>
    for {
      boxId               <- c.downField("boxId").as[BoxId]
      value               <- c.downField("value").as[Long]
      creationHeight      <- c.downField("creationHeight").as[Int]
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

}

final case class ApiPowSolutions(pk: HexString, w: HexString, n: HexString, d: String)

object ApiPowSolutions {

  implicit val jsonDecoder: Decoder[ApiPowSolutions] = { (c: HCursor) =>
    for {
      pk <- c.downField("pk").as[HexString]
      w  <- c.downField("w").as[HexString]
      n  <- c.downField("n").as[HexString]
      d  <- c.downField("d").as[BigInt]
    } yield ApiPowSolutions(pk, w, n, d.toString())
  }
}

final case class ApiSpendingProof(proofBytes: Option[HexString], extension: Json)

object ApiSpendingProof {

  implicit val decoder: Decoder[ApiSpendingProof] = { (c: HCursor) =>
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
}

final case class ApiInput(boxId: BoxId, spendingProof: ApiSpendingProof)

final case class ApiTransaction(
  id: TxId,
  inputs: ArraySeq[ApiInput],
  dataInputs: List[ApiDataInput],
  outputs: ArraySeq[ApiOutput],
  size: Int
)

object ApiTransaction {
  import io.circe.generic.auto.*

  implicit def jsonDecoder(implicit enc: ErgoAddressEncoder): Decoder[ApiTransaction] = { (c: HCursor) =>
    for {
      id         <- c.downField("id").as[TxId]
      inputs     <- c.downField("inputs").as[ArraySeq[ApiInput]]
      dataInputs <- c.downField("dataInputs").as[List[ApiDataInput]]
      outputs    <- c.downField("outputs").as[ArraySeq[ApiOutput]]
      size       <- c.downField("size").as[Int]
    } yield ApiTransaction(id, inputs, dataInputs, outputs, size)
  }

}

final case class ExpandedRegister(
  serializedValue: HexString,
  regValue: Option[RegisterValue]
)

final case class RegisterValue(sigmaType: SigmaType, value: String)

final case class TokenProps(
  name: String,
  description: String,
  decimals: Int
)
