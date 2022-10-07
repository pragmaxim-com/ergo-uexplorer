package org.ergoplatform.uexplorer

import org.ergoplatform.uexplorer.node.{ExpandedRegister, RegisterValue}
import sigmastate.Values.EvaluatedValue
import sigmastate._
import sigmastate.serialization.ValueSerializer

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object RegistersParser {

  def parseAny(raw: HexString): Try[RegisterValue] =
    Try(ValueSerializer.deserialize(raw.bytes)).flatMap {
      case v: EvaluatedValue[_] =>
        sigma
          .renderEvaluatedValue(v)
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
