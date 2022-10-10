package org.ergoplatform.uexplorer

import cats.Eval
import cats.data.{NonEmptyList, OptionT}
import cats.instances.list.*
import cats.syntax.either.*
import cats.syntax.traverse.*
import io.circe.syntax.*
import io.circe.{Decoder, DecodingFailure, Encoder}

import scala.annotation.tailrec
import scala.util.Try

trait SigmaType

object SigmaType {

  enum SimpleKindSigmaType extends SigmaType {
    case SBoolean
    case SByte
    case SShort
    case SInt
    case SLong
    case SBigInt
    case SContext
    case SGlobal
    case SHeader
    case SPreHeader
    case SAvlTree
    case SGroupElement
    case SSigmaProp
    case SString
    case SBox
    case SUnit
    case SAny
  }

  sealed abstract class HigherKinded1SigmaType extends SigmaType {
    val typeParam: SigmaType
  }

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
    def in(si: String): Option[SigmaType] =
      Try(SimpleKindSigmaType.valueOf(si)).toOption.orElse {
        si match {
          case HKType1Pattern(tpe, tParamRaw) =>
            in(tParamRaw).flatMap { tParam =>
              tpe match {
                case "Coll"   => Option(SCollection(tParam))
                case "Option" => Option(SOption(tParam))
                case _        => Option.empty
              }
            }
          case TupleNTypePattern(tps) =>
            parseHighLevelTypeSigns(tps).traverse(in).map(xs => STupleN(NonEmptyList.fromListUnsafe(xs)))
          case _ => Option.empty
        }
      }

    in(s)
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
        case st: SimpleKindSigmaType => Eval.now(st.toString)
        case coll: SCollection       => Eval.defer(go(coll.typeParam)).map(r => s"Coll[$r]")
        case opt: SOption            => Eval.defer(go(opt.typeParam)).map(r => s"Option[$r]")
        case STupleN(tParams) =>
          tParams.traverse(tp => Eval.defer(go(tp))).map(tps => "(" + tps.toList.mkString(", ") + ")")
      }

    go(t).value
  }
}
