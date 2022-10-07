package org.ergoplatform.uexplorer.indexer.parser

import cats.Eval
import cats.data.OptionT
import cats.implicits.toTraverseOps
import org.ergoplatform.uexplorer.node.{ExpandedRegister, RegisterValue}
import org.ergoplatform.uexplorer.{HexString, RegisterId, SigmaType}
import scorex.util.encode.Base16
import sigmastate.Values.{Constant, ConstantNode, EvaluatedValue, SigmaPropConstant}
import sigmastate._
import sigmastate.basics.DLogProtocol.ProveDlogProp
import sigmastate.serialization.ValueSerializer

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object RegistersParser {

  @inline def renderEvaluatedValue[T <: SType](ev: EvaluatedValue[T]): Option[(SigmaType, String)] = {
    def goRender[T0 <: SType](ev0: EvaluatedValue[T0]): OptionT[Eval, (SigmaType, String)] =
      ev0.tpe match {
        case SSigmaProp | SGroupElement =>
          ev0 match {
            case SigmaPropConstant(ProveDlogProp(dlog)) =>
              OptionT.some(SigmaType.SimpleKindSigmaType.SSigmaProp -> Base16.encode(dlog.pkBytes))
            case ConstantNode(groupElem, SGroupElement) =>
              OptionT.some(
                SigmaType.SimpleKindSigmaType.SGroupElement ->
                Base16.encode(groupElem.asInstanceOf[SGroupElement.WrappedType].getEncoded.toArray)
              )
            case _ => OptionT.none
          }
        case prim: SPrimType =>
          val typeTerm = prim.toString.replaceAll("\\$", "")
          OptionT.fromOption[Eval](SigmaType.parse(typeTerm)).map(_ -> ev0.value.toString)
        case tuple: STuple =>
          val typeTerm = tuple.toString.replaceAll("\\$", "")
          OptionT.fromOption[Eval](SigmaType.parse(typeTerm)).flatMap { tp =>
            val untypedElems = ev0.value match {
              case (a, b) => List(a, b)
              case _      => ev0.value.asInstanceOf[tuple.WrappedType].toArray.toList
            }
            val elems =
              untypedElems.zip(tuple.items).map { case (vl, tp) =>
                Constant[SType](vl.asInstanceOf[tp.WrappedType], tp)
              }
            elems.traverse(e => goRender(e).map(_._2)).map { xs =>
              tp -> ("[" + xs.mkString(",") + "]")
            }
          }
        case SCollectionType(SByte) =>
          OptionT.some(
            SigmaType.SCollection(SigmaType.SimpleKindSigmaType.SByte) ->
            Base16.encode(ev0.value.asInstanceOf[SCollection[SByte.type]#WrappedType].toArray)
          )
        case coll: SCollection[_] =>
          val typeTerm = coll.toString.replaceAll("\\$", "")
          OptionT.fromOption[Eval](SigmaType.parse(typeTerm)).flatMap { tp =>
            val elems = ev0.value.asInstanceOf[coll.WrappedType].toArray.toList.map(Constant(_, coll.elemType))
            elems.traverse(e => goRender(e).map(_._2)).map { xs =>
              tp -> ("[" + xs.mkString(",") + "]")
            }
          }
        case option: SOption[_] =>
          OptionT.fromOption[Eval](SigmaType.parse(option.toTermString)).flatMap { tp =>
            val elem = ev0.value.asInstanceOf[option.WrappedType].map(Constant(_, option.elemType))
            elem match {
              case Some(value) => OptionT(Eval.defer(goRender(value).value)).map(r => tp -> r._2)
              case None        => OptionT.some(tp -> "null")
            }
          }
        case _ =>
          OptionT.none
      }

    goRender(ev).value.value
  }

  def parseAny(raw: HexString): Try[RegisterValue] =
    Try(ValueSerializer.deserialize(raw.bytes)).flatMap {
      case v: EvaluatedValue[_] =>
        renderEvaluatedValue(v)
          .map { case (tp, vl) => Try(RegisterValue(tp, vl)) }
          .getOrElse(Failure(new Exception(s"Failed to render constant value [$v] in register")))
      case v =>
        Failure(new Exception(s"Got non constant value [$v] in register"))
    }

  def parse[T <: SType](raw: HexString)(implicit ev: ClassTag[T#WrappedType]): Try[T#WrappedType] =
    Try(ValueSerializer.deserialize(raw.bytes)).flatMap {
      case v: EvaluatedValue[_] =>
        v.value match {
          case wrappedValue: T#WrappedType =>
            Success(wrappedValue)
          case wrappedValue =>
            Failure(new Exception(s"Got wrapped value [$wrappedValue] of unexpected type in register"))
        }
      case v =>
        Failure(new Exception(s"Got non constant value [$v] in register"))
    }

  /** Expand registers into `register_id -> expanded_register` form.
    */
  @inline def expand(registers: Map[RegisterId, HexString]): Map[RegisterId, ExpandedRegister] = {
    val expanded =
      for {
        (idSig, serializedValue) <- registers.toList
        rv = parseAny(serializedValue).toOption
      } yield idSig -> ExpandedRegister(serializedValue, rv.map(_.sigmaType), rv.map(_.value))
    expanded.toMap
  }

}
