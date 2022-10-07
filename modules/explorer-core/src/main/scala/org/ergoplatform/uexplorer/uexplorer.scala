package org.ergoplatform

import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.{HexStringSpec, MatchesRegex, Url}
import eu.timepit.refined.{refineV, W}
import io.circe.refined._
import io.circe.{Decoder, Encoder}
import io.estatico.newtype.macros.newtype
import io.estatico.newtype.ops._
import org.ergoplatform.uexplorer.constraints._
import pureconfig.ConfigReader
import scorex.util.encode.Base16

package object uexplorer {

  object constraints {

    type OrderingSpec = MatchesRegex[W.`"^(?i)(asc|desc)$"`.T]

    type OrderingString = String Refined OrderingSpec

    type Base58Spec = MatchesRegex[W.`"[1-9A-HJ-NP-Za-km-z]+"`.T]

    type AddressType = String Refined Base58Spec

    type HexStringType = String Refined HexStringSpec

    type UrlStringType = String Refined Url
  }

  /** Persistent modifier id (header, block_transaction, etc.)
    */
  @newtype case class BlockId(value: HexString)

  object BlockId {
    implicit def encoder: Encoder[BlockId]   = deriving
    implicit def decoder: Decoder[BlockId]   = deriving
    def fromStringUnsafe(s: String): BlockId = unsafeWrap(HexString.fromStringUnsafe(s))
  }

  @newtype case class TxId(value: String)

  object TxId {
    implicit def encoder: Encoder[TxId] = deriving
    implicit def decoder: Decoder[TxId] = deriving
  }

  @newtype case class BoxId(value: String)

  object BoxId {
    implicit def encoder: Encoder[BoxId] = deriving
    implicit def decoder: Decoder[BoxId] = deriving

    def fromErgo(boxId: org.ergoplatform.ErgoBox.BoxId): BoxId =
      BoxId(Base16.encode(boxId))
  }

  @newtype case class TokenId(value: HexString)

  object TokenId {
    implicit def encoder: Encoder[TokenId] = deriving
    implicit def decoder: Decoder[TokenId] = deriving

    def fromStringUnsafe(s: String): TokenId = unsafeWrap(HexString.fromStringUnsafe(s))
  }

  @newtype case class TokenName(value: String)

  object TokenName {
    implicit def encoder: Encoder[TokenName]           = deriving
    implicit def decoder: Decoder[TokenName]           = deriving
    def fromStringUnsafe(tokenName: String): TokenName = TokenName(tokenName)
  }

  @newtype case class TokenSymbol(value: String)

  object TokenSymbol {
    implicit def encoder: Encoder[TokenSymbol]   = deriving
    implicit def decoder: Decoder[TokenSymbol]   = deriving
    def fromStringUnsafe(s: String): TokenSymbol = TokenSymbol(s)
  }

  @newtype case class ErgoTreeTemplateHash(value: HexString)

  object ErgoTreeTemplateHash {
    implicit def encoder: Encoder[ErgoTreeTemplateHash]   = deriving
    implicit def decoder: Decoder[ErgoTreeTemplateHash]   = deriving
    def fromStringUnsafe(s: String): ErgoTreeTemplateHash = unsafeWrap(HexString.fromStringUnsafe(s))
  }

  @newtype case class ErgoTree(value: HexString)

  object ErgoTree {
    implicit val encoder: io.circe.Encoder[ErgoTree] = deriving
    implicit val decoder: io.circe.Decoder[ErgoTree] = deriving
  }

  @newtype case class TokenType(value: String)

  object TokenType {

    val Eip004: TokenType                    = "EIP-004".coerce[TokenType]
    implicit def encoder: Encoder[TokenType] = deriving
    implicit def decoder: Decoder[TokenType] = deriving
  }

  // Ergo Address
  @newtype case class Address(value: AddressType) {
    final def unwrapped: String = value.value
  }

  object Address {
    implicit def encoder: Encoder[Address] = deriving
    implicit def decoder: Decoder[Address] = deriving

    implicit def configReader: ConfigReader[Address] =
      implicitly[ConfigReader[String]].map(fromStringUnsafe)

    def fromStringUnsafe(s: String): Address = unsafeWrap(refineV[Base58Spec].unsafeFrom(s))
  }

  @newtype case class HexString(value: HexStringType) {
    final def unwrapped: String  = value.value
    final def bytes: Array[Byte] = Base16.decode(unwrapped).get
  }

  object HexString {
    implicit def encoder: Encoder[HexString]   = deriving
    implicit def decoder: Decoder[HexString]   = deriving
    def fromStringUnsafe(s: String): HexString = unsafeWrap(refineV[HexStringSpec].unsafeFrom(s))
  }

  @newtype case class UrlString(value: UrlStringType) {
    final def unwrapped: String = value.value
  }

  object UrlString {
    implicit def encoder: Encoder[UrlString] = deriving
    implicit def decoder: Decoder[UrlString] = deriving

    implicit def configReader: ConfigReader[UrlString] =
      implicitly[ConfigReader[String]].map(fromStringUnsafe)

    def fromStringUnsafe(s: String): UrlString = unsafeWrap(refineV[Url].unsafeFrom(s))
  }

}
