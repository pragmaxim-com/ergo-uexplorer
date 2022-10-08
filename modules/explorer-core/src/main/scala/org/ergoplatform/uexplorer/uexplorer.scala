package org.ergoplatform

import enumeratum.{CirceEnum, Enum, EnumEntry}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.{HexStringSpec, MatchesRegex}
import eu.timepit.refined.{refineV, W}
import io.circe.refined._
import io.circe.{Decoder, Encoder, KeyDecoder, KeyEncoder}
import io.estatico.newtype.macros.newtype
import io.estatico.newtype.ops._
import org.ergoplatform.uexplorer.constraints._

package object uexplorer {

  object constraints {

    type Base58Spec = MatchesRegex[W.`"[1-9A-HJ-NP-Za-km-z]+"`.T]

    type AddressType = String Refined Base58Spec

    type HexStringType = String Refined HexStringSpec

  }

  /** Persistent modifier id (header, block_transaction, etc.)
    */
  @newtype case class BlockId(value: HexString)

  object BlockId {
    implicit def encoder: Encoder[BlockId] = deriving

    implicit def decoder: Decoder[BlockId] = deriving

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

  }

  @newtype case class TokenId(value: HexString)

  object TokenId {
    implicit def encoder: Encoder[TokenId] = deriving

    implicit def decoder: Decoder[TokenId] = deriving

    def fromStringUnsafe(s: String): TokenId = unsafeWrap(HexString.fromStringUnsafe(s))
  }

  @newtype case class TokenName(value: String)

  object TokenName {
    implicit def encoder: Encoder[TokenName] = deriving

    implicit def decoder: Decoder[TokenName] = deriving

    def fromStringUnsafe(tokenName: String): TokenName = TokenName(tokenName)
  }

  @newtype case class TokenSymbol(value: String)

  object TokenSymbol {
    implicit def encoder: Encoder[TokenSymbol] = deriving

    implicit def decoder: Decoder[TokenSymbol] = deriving

    def fromStringUnsafe(s: String): TokenSymbol = TokenSymbol(s)
  }

  @newtype case class ErgoTreeTemplateHash(value: HexString)

  object ErgoTreeTemplateHash {
    implicit def encoder: Encoder[ErgoTreeTemplateHash] = deriving

    implicit def decoder: Decoder[ErgoTreeTemplateHash] = deriving

    def fromStringUnsafe(s: String): ErgoTreeTemplateHash = unsafeWrap(HexString.fromStringUnsafe(s))
  }

  @newtype case class ErgoTree(value: HexString)

  object ErgoTree {
    implicit val encoder: io.circe.Encoder[ErgoTree] = deriving
    implicit val decoder: io.circe.Decoder[ErgoTree] = deriving
  }

  @newtype case class TokenType(value: String)

  object TokenType {

    val Eip004: TokenType = "EIP-004".coerce[TokenType]

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

    def fromStringUnsafe(s: String): Address = unsafeWrap(refineV[Base58Spec].unsafeFrom(s))
  }

  @newtype case class HexString(value: HexStringType) {
    final def unwrapped: String = value.value

  }

  object HexString {
    implicit def encoder: Encoder[HexString] = deriving

    implicit def decoder: Decoder[HexString] = deriving

    def fromStringUnsafe(s: String): HexString = unsafeWrap(refineV[HexStringSpec].unsafeFrom(s))
  }

  sealed abstract class RegisterId extends EnumEntry

  object RegisterId extends Enum[RegisterId] with CirceEnum[RegisterId] {

    case object R0 extends RegisterId
    case object R1 extends RegisterId
    case object R2 extends RegisterId
    case object R3 extends RegisterId
    case object R4 extends RegisterId
    case object R5 extends RegisterId
    case object R6 extends RegisterId
    case object R7 extends RegisterId
    case object R8 extends RegisterId
    case object R9 extends RegisterId

    val values = findValues

    implicit val keyDecoder: KeyDecoder[RegisterId] = withNameOption
    implicit val keyEncoder: KeyEncoder[RegisterId] = _.entryName

  }

  trait SigmaType

  object SigmaType {
    import enumeratum._

    sealed abstract class SimpleKindSigmaType extends EnumEntry with SigmaType

    object SimpleKindSigmaType extends Enum[SimpleKindSigmaType] {
      case object SBoolean extends SimpleKindSigmaType
      case object SByte extends SimpleKindSigmaType
      case object SShort extends SimpleKindSigmaType
      case object SInt extends SimpleKindSigmaType
      case object SLong extends SimpleKindSigmaType
      case object SBigInt extends SimpleKindSigmaType
      case object SContext extends SimpleKindSigmaType
      case object SGlobal extends SimpleKindSigmaType
      case object SHeader extends SimpleKindSigmaType
      case object SPreHeader extends SimpleKindSigmaType
      case object SAvlTree extends SimpleKindSigmaType
      case object SGroupElement extends SimpleKindSigmaType
      case object SSigmaProp extends SimpleKindSigmaType
      case object SString extends SimpleKindSigmaType
      case object SBox extends SimpleKindSigmaType
      case object SUnit extends SimpleKindSigmaType
      case object SAny extends SimpleKindSigmaType

      val values = findValues
    }

    import cats.Eval
    import cats.data.{NonEmptyList, OptionT}
    import cats.instances.list._
    import cats.syntax.either._
    import cats.syntax.traverse._
    import io.circe.syntax._
    import io.circe.{Decoder, DecodingFailure, Encoder}
    import scala.annotation.tailrec

    sealed abstract class HigherKinded1SigmaType extends SigmaType { val typeParam: SigmaType }

    final case class SCollection(typeParam: SigmaType) extends HigherKinded1SigmaType
    final case class SOption(typeParam: SigmaType) extends HigherKinded1SigmaType

    final case class STupleN(typeParams: NonEmptyList[SigmaType]) extends SigmaType

    implicit def encoder: Encoder[SigmaType] = render(_).asJson

    implicit def decoder: Decoder[SigmaType] =
      c =>
        c.as[String]
          .flatMap { s =>
            parse(s).fold(DecodingFailure(s"Unknown SigmaType signature: [$s]", c.history).asLeft[SigmaType])(_.asRight)
          }

    private val HKType1Pattern    = "^([a-zA-Z]+)\\[(.+)\\]$".r
    private val TupleNTypePattern = "^\\(((?:\\S+,?.)+)\\)$".r

    def parse(s: String): Option[SigmaType] = {
      def in(si: String): OptionT[Eval, SigmaType] =
        OptionT.fromOption[Eval](SimpleKindSigmaType.withNameOption(si): Option[SigmaType]).orElse {
          si match {
            case HKType1Pattern(tpe, tParamRaw) =>
              in(tParamRaw).flatMap { tParam =>
                tpe match {
                  case "Coll"   => OptionT.some(SCollection(tParam))
                  case "Option" => OptionT.some(SOption(tParam))
                  case _        => OptionT.none
                }
              }
            case TupleNTypePattern(tps) =>
              parseHighLevelTypeSigns(tps).traverse(in).map(xs => STupleN(NonEmptyList.fromListUnsafe(xs)))
            case _ => OptionT.none
          }
        }
      in(s).value.value
    }

    private def parseHighLevelTypeSigns(s: String): List[String] = {
      @tailrec def go(
        rem: List[Char],
        bf: List[Char],
        acc: List[String],
        openBrackets: Int,
        closedBrackets: Int
      ): List[String] =
        rem match {
          case h :: tl =>
            h match {
              case '[' => go(tl, bf :+ h, acc, openBrackets + 1, closedBrackets)
              case ']' => go(tl, bf :+ h, acc, openBrackets, closedBrackets + 1)
              case ',' if openBrackets == closedBrackets =>
                go(tl, List.empty, acc :+ bf.mkString.trim, openBrackets, closedBrackets)
              case c => go(tl, bf :+ c, acc, openBrackets, closedBrackets)
            }
          case _ => acc :+ bf.mkString.trim
        }
      go(s.toList, List.empty, List.empty, openBrackets = 0, closedBrackets = 0)
    }

    private def render(t: SigmaType): String = {
      def go(t0: SigmaType): Eval[String] =
        t0 match {
          case st: SimpleKindSigmaType => Eval.now(st.entryName)
          case coll: SCollection       => Eval.defer(go(coll.typeParam)).map(r => s"Coll[$r]")
          case opt: SOption            => Eval.defer(go(opt.typeParam)).map(r => s"Option[$r]")
          case STupleN(tParams) =>
            tParams.traverse(tp => Eval.defer(go(tp))).map(tps => "(" + tps.toList.mkString(", ") + ")")
        }
      go(t).value
    }
  }

}
