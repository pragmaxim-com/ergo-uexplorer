package org.ergoplatform

import eu.timepit.refined.api.RefType.tagRefType.unsafeWrap
import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.{HexStringSpec, MatchesRegex, ValidByte}
import eu.timepit.refined.refineV
import io.circe.*
import org.ergoplatform.uexplorer.{BoxCount, BoxId, LastHeight, TxCount}
import scorex.crypto.hash.Digest32
import eu.timepit.refined.auto.autoUnwrap

import scala.collection.mutable
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.{ArraySeq, TreeMap}
import scala.collection.immutable.ListMap
import scala.util.Try
import zio.json.*
import zio.*
import zio.json.interop.refined.*

package object uexplorer {

  type Value          = Long
  type Amount         = Long
  type Height         = Int
  type CreationHeight = Int
  type Timestamp      = Long

  type MinerReward = Long
  type MinerFee    = Long

  type BoxCount   = Int
  type TxCount    = Int
  type LastHeight = Int

  type Revision = Long

  type Base58Spec    = MatchesRegex["[1-9A-HJ-NP-Za-km-z]+"]
  type NetworkPrefix = String Refined ValidByte

  type Address = String Refined Base58Spec
  object Address {
    extension (x: Address) def unwrappedAddress: String = x
    def fromStringUnsafe(s: String): Address            = unsafeWrap(refineV[Base58Spec].unsafeFrom(s))
    def castUnsafe(s: String): Address                  = s.asInstanceOf[Address]
  }

  object NetworkPrefix {
    def fromStringUnsafe(s: String): NetworkPrefix = unsafeWrap(refineV[ValidByte].unsafeFrom(s))
  }

  type HexString           = String Refined HexStringSpec
  type AdProofsRootHex     = HexString
  type AvlTreePathProofHex = HexString
  type TreeRootHashHex     = HexString
  type ExtensionDigestHex  = HexString
  type BoxRegisterValueHex = HexString
  type StateRootHex        = HexString
  type TransactionsRootHex = HexString
  type PowHex              = HexString
  type PowNonceHex         = HexString
  type InputProofHex       = HexString

  object HexString {
    extension (x: HexString) def unwrapped: String = x
    def fromStringUnsafe(s: String): HexString     = unsafeWrap(refineV[HexStringSpec].unsafeFrom(s))
    def castUnsafe(s: String): HexString           = s.asInstanceOf[HexString]
    given Ordering[HexString]                      = Ordering.by[HexString, String](_.unwrapped)
  }

  object AvlTreePathProofHex {
    def fromStringUnsafe(s: String): AvlTreePathProofHex = unsafeWrap(refineV[HexStringSpec].unsafeFrom(s))
  }

  type BlockId = HexString
  object BlockId {
    def fromStringUnsafe(s: String): BlockId     = unsafeWrap(HexString.fromStringUnsafe(s))
    def castUnsafe(s: String): BlockId           = s.asInstanceOf[BlockId]
    given JsonEncoder[BlockId]                   = JsonEncoder.string.contramap(BlockId.fromStringUnsafe)
    extension (x: BlockId) def unwrapped: String = x
  }

  type TokenId = HexString

  object TokenId {
    extension (x: TokenId) def unwrapped: String = x
    def fromStringUnsafe(s: String): TokenId     = unsafeWrap(HexString.fromStringUnsafe(s))
  }

  type ErgoTreeHex = HexString
  object ErgoTreeHex {
    def fromStringUnsafe(s: String): ErgoTreeHex = unsafeWrap(refineV[HexStringSpec].unsafeFrom(s))
    def castUnsafe(s: String): ErgoTreeHex       = s.asInstanceOf[ErgoTreeHex]
  }

  type ErgoTreeT8Hex = HexString
  object ErgoTreeT8Hex {
    def fromStringUnsafe(s: String): ErgoTreeT8Hex = unsafeWrap(HexString.fromStringUnsafe(s))
    def castUnsafe(s: String): ErgoTreeHex         = s.asInstanceOf[ErgoTreeHex]
  }

  type ErgoTreeHash = HexString

  object ErgoTreeHash {
    def fromStringUnsafe(s: String): HexString = unsafeWrap(refineV[HexStringSpec].unsafeFrom(s))

    def castUnsafe(s: String): HexString = s.asInstanceOf[HexString]
  }

  type ErgoTreeT8Hash = HexString

  object ErgoTreeT8Hash {
    def fromStringUnsafe(s: String): ErgoTreeT8Hash = unsafeWrap(HexString.fromStringUnsafe(s))

    def castUnsafe(s: String): ErgoTreeT8Hash = s.asInstanceOf[ErgoTreeT8Hash]
  }

  opaque type TxId = String

  object TxId {
    def apply(s: String): TxId                = s
    given Encoder[TxId]                       = Encoder.encodeString
    given Decoder[TxId]                       = Decoder.decodeString
    given JsonEncoder[TxId]                   = JsonEncoder.string
    given JsonDecoder[TxId]                   = JsonDecoder.string
    extension (x: TxId) def unwrapped: String = x
    def castUnsafe(s: String): TxId           = s.asInstanceOf[TxId]
  }

  opaque type BoxId = String

  object BoxId {
    def apply(s: String): BoxId = s

    given Encoder[BoxId]                       = Encoder.encodeString
    given JsonEncoder[BoxId]                   = JsonEncoder.string
    given JsonDecoder[BoxId]                   = JsonDecoder.string
    given Decoder[BoxId]                       = Decoder.decodeString
    extension (x: BoxId) def unwrapped: String = x
    def castUnsafe(s: String): BoxId           = s.asInstanceOf[BoxId]
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

  implicit class MapPimp[K, V](underlying: Map[K, V]) {

    def putOrRemove(k: K)(f: Option[V] => Option[V]): Map[K, V] =
      f(underlying.get(k)) match {
        case None    => underlying removed k
        case Some(v) => underlying updated (k, v)
      }

    def adjust(k: K)(f: Option[V] => V): Map[K, V] = underlying.updated(k, f(underlying.get(k)))
  }

  implicit class MutableMapPimp[K, V](underlying: mutable.Map[K, V]) {

    def putOrRemove(k: K)(f: Option[V] => Option[V]): mutable.Map[K, V] =
      f(underlying.get(k)) match {
        case None => underlying -= k
        case Some(v) =>
          underlying.put(k, v)
          underlying
      }

    def adjust(k: K)(f: Option[V] => V): mutable.Map[K, V] = {
      underlying.put(k, f(underlying.get(k)))
      underlying
    }
  }

}
