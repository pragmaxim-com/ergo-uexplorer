package org.ergoplatform.uexplorer.backend

import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}

case class ErrorResponse(error: Int, reason: String, detail: String = "unknown")

object ErrorResponse {
  implicit val encoder: JsonEncoder[ErrorResponse] = DeriveJsonEncoder.gen[ErrorResponse]
  implicit val decode: JsonDecoder[ErrorResponse]  = DeriveJsonDecoder.gen[ErrorResponse]
}

case class IdParsingException(id: String, message: String) extends Throwable(message)
