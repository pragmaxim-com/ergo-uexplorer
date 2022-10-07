package org.ergoplatform.uexplorer.node

import cats.syntax.option._
import io.circe.{Decoder, DecodingFailure, HCursor, Json}
import org.ergoplatform.uexplorer.HexString

import scala.util.{Failure, Success, Try}

/** A model mirroring SpendingProof entity from Ergo node REST API.
  * See `SpendingProof` in https://github.com/ergoplatform/ergo/blob/master/src/main/resources/api/openapi.yaml
  */
final case class ApiSpendingProof(proofBytes: Option[HexString], extension: Json)

object ApiSpendingProof {

  implicit val decoder: Decoder[ApiSpendingProof] = { c: HCursor =>
    for {
      proofBytes <- c.downField("proofBytes").as[String].flatMap { s =>
                      Try(HexString.fromStringUnsafe(s)) match {
                        case Failure(_)     => Right[DecodingFailure, Option[HexString]](none)
                        case Success(value) => Right[DecodingFailure, Option[HexString]](Option(value))
                      }
                    }
      extension <- c.downField("extension").as[Json]
    } yield ApiSpendingProof(proofBytes, extension)
  }
}
