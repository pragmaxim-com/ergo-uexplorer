package org.ergoplatform.uexplorer.backend

import io.getquill.MappedEncoding
import org.ergoplatform.uexplorer.Address.unwrappedAddress
import org.ergoplatform.uexplorer.HexString.unwrapped
import org.ergoplatform.uexplorer.backend.boxes.*
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.*
import sttp.model.StatusCode
import sttp.tapir.Schema
import zio.json.*

import scala.util.Try

trait Codecs {

  def handleThrowable(t: Throwable): (ErrorResponse, StatusCode) = t match {
    case IdParsingException(e, msg) =>
      ErrorResponse(StatusCode.BadRequest.code, msg) -> StatusCode.BadRequest
    case e: Throwable =>
      ErrorResponse(StatusCode.InternalServerError.code, e.getMessage) -> StatusCode.InternalServerError
  }

  given Schema[Address]     = Schema.string
  given Schema[TokenType]   = Schema.string
  given Schema[BoxId]       = Schema.string
  given Schema[BlockId]     = Schema.string
  given Schema[TxId]        = Schema.string
  given Schema[Reg]         = Schema.string
  given Schema[BoxRegister] = Schema.derived
  given Schema[SigmaType]   = Schema.string[String].map[SigmaType](SigmaType.parse)(s => SigmaType.encoder(s).noSpaces.trim.replaceAll("\"", ""))

  given MappedEncoding[HexString, String] = MappedEncoding[HexString, String](_.unwrapped)
  given MappedEncoding[String, HexString] = MappedEncoding[String, HexString](HexString.castUnsafe)
  given MappedEncoding[Address, String]   = MappedEncoding[Address, String](_.unwrappedAddress)
  given MappedEncoding[String, Address]   = MappedEncoding[String, Address](Address.castUnsafe)
  given MappedEncoding[BoxId, String]     = MappedEncoding[BoxId, String](_.unwrapped)
  given MappedEncoding[String, BoxId]     = MappedEncoding[String, BoxId](BoxId.castUnsafe)
  given MappedEncoding[TxId, String]      = MappedEncoding[TxId, String](_.unwrapped)
  given MappedEncoding[String, TxId]      = MappedEncoding[String, TxId](TxId.castUnsafe)
  given MappedEncoding[TokenType, String] = MappedEncoding[TokenType, String](_.unwrapped)
  given MappedEncoding[String, TokenType] = MappedEncoding[String, TokenType](TokenType.castUnsafe)

  // json
  given JsonEncoder[TokenType] = JsonEncoder[String].contramap(_.unwrapped)
  given JsonDecoder[TokenType] = JsonDecoder[String].map(TokenType.castUnsafe)
  given JsonEncoder[HexString] = JsonEncoder[String].contramap(_.unwrapped)
  given JsonDecoder[HexString] = JsonDecoder[String].map(HexString.castUnsafe)
  given JsonEncoder[Address]   = JsonEncoder[String].contramap(_.unwrappedAddress)
  given JsonDecoder[Address]   = JsonDecoder[String].map(Address.castUnsafe)
  given JsonEncoder[BoxId]     = JsonEncoder[String].contramap(_.unwrapped)
  given JsonDecoder[BoxId]     = JsonDecoder[String].map(BoxId(_))
  given JsonEncoder[TxId]      = JsonEncoder[String].contramap(_.unwrapped)
  given JsonDecoder[TxId]      = JsonDecoder[String].map(TxId(_))
  given JsonDecoder[Utxo]      = DeriveJsonDecoder.gen[Utxo]
  given JsonDecoder[Box]       = DeriveJsonDecoder.gen[Box]
  given JsonEncoder[Utxo]      = DeriveJsonEncoder.gen[Utxo]
  given JsonEncoder[Box]       = DeriveJsonEncoder.gen[Box]
  given JsonEncoder[Block]     = DeriveJsonEncoder.gen[Block]
  given JsonDecoder[Block]     = DeriveJsonDecoder.gen[Block]

}
