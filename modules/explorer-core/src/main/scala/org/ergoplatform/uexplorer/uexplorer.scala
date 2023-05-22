package org.ergoplatform

import eu.timepit.refined.api.RefType.tagRefType.unsafeWrap
import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.{HexStringSpec, MatchesRegex, ValidByte}
import eu.timepit.refined.refineV
import io.circe.*
import org.ergoplatform.uexplorer.{BoxCount, LastHeight, TxCount}

import scala.collection.immutable.ListMap
import scala.util.Try

package object uexplorer {

  type Value      = Long
  type Height     = Int
  type EpochIndex = Int
  type TxIndex    = Short

  type BoxCount            = Int
  type TxCount             = Int
  type LastHeight          = Int
  type TopAddressMap       = Map[Address, Address.Stats]
  type SortedTopAddressMap = ListMap[Address, Address.Stats]

  type Base58Spec    = MatchesRegex["[1-9A-HJ-NP-Za-km-z]+"]
  type Address       = String Refined Base58Spec
  type NetworkPrefix = String Refined ValidByte

  object Address {
    case class Stats(lastTxHeight: LastHeight, txCount: TxCount, boxCount: BoxCount)
    case class State(value: Value, stats: Option[Address.Stats])
    def fromStringUnsafe(s: String): Address = unsafeWrap(refineV[Base58Spec].unsafeFrom(s))
  }

  object NetworkPrefix {
    def fromStringUnsafe(s: String): NetworkPrefix = unsafeWrap(refineV[ValidByte].unsafeFrom(s))
  }

  type HexString = String Refined HexStringSpec

  object HexString {
    def fromStringUnsafe(s: String): HexString = unsafeWrap(refineV[HexStringSpec].unsafeFrom(s))
  }

  type BlockId = String Refined HexStringSpec

  object BlockId {
    def fromStringUnsafe(s: String): BlockId = unsafeWrap(HexString.fromStringUnsafe(s))
  }

  type TokenId = String Refined HexStringSpec

  object TokenId {
    def fromStringUnsafe(s: String): TokenId = unsafeWrap(HexString.fromStringUnsafe(s))
  }

  type ErgoTreeTemplateHash = String Refined HexStringSpec

  object ErgoTreeTemplateHash {
    def fromStringUnsafe(s: String): ErgoTreeTemplateHash = unsafeWrap(HexString.fromStringUnsafe(s))
  }

  type ErgoTree = String Refined HexStringSpec

  object ErgoTree {
    def fromStringUnsafe(s: String): ErgoTree = unsafeWrap(refineV[HexStringSpec].unsafeFrom(s))
  }

  opaque type TxId = String

  object TxId {
    def apply(s: String): TxId = s

    given Encoder[TxId] = Encoder.encodeString

    given Decoder[TxId]                       = Decoder.decodeString
    extension (x: TxId) def unwrapped: String = x
  }

  opaque type BoxId = String

  object BoxId {
    def apply(s: String): BoxId = s

    given Encoder[BoxId] = Encoder.encodeString

    given Decoder[BoxId]                       = Decoder.decodeString
    extension (x: BoxId) def unwrapped: String = x
  }

  opaque type TokenName = String

  object TokenName {
    def apply(s: String): TokenName = s

    given Encoder[TokenName] = Encoder.encodeString

    given Decoder[TokenName] = Decoder.decodeString

    extension (x: TokenName) def unwrapped: String = x
  }

  opaque type TokenSymbol = String

  object TokenSymbol {
    def apply(s: String): TokenSymbol = s

    given Encoder[TokenSymbol] = Encoder.encodeString

    given Decoder[TokenSymbol] = Decoder.decodeString

    extension (x: TokenSymbol) def unwrapped: String = x
  }

  opaque type TokenType = String

  object TokenType {
    def apply(s: String): TokenType = s

    val Eip004: TokenType = TokenType("EIP-004")

    given Encoder[TokenType] = Encoder.encodeString

    given Decoder[TokenType] = Decoder.decodeString

    extension (x: TokenType) def unwrapped: String = x

  }

  enum RegisterId {
    case R0
    case R1
    case R2
    case R3
    case R4
    case R5
    case R6
    case R7
    case R8
    case R9
  }

  object RegisterId {
    given keyEncoder: KeyEncoder[RegisterId] = (a: RegisterId) => a.toString
    given keyDecoder: KeyDecoder[RegisterId] = KeyDecoder.decodeKeyString.map(RegisterId.valueOf)
  }

}
