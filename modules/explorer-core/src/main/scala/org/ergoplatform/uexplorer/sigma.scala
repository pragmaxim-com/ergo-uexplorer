package org.ergoplatform.uexplorer

import cats.Eval
import cats.data.OptionT
import cats.implicits.toTraverseOps
import org.ergoplatform.uexplorer.Err.RequestProcessingErr.DexErr.ContractParsingErr.Base16DecodingFailed
import org.ergoplatform.uexplorer.Err.RequestProcessingErr.DexErr.ContractParsingErr.ErgoTreeSerializationErr.ErgoTreeDeserializationFailed
import org.ergoplatform.{uexplorer, ErgoAddress, ErgoAddressEncoder, Pay2SAddress}
import scorex.crypto.hash.Sha256
import scorex.util.encode.Base16
import sigmastate.Values.{Constant, ConstantNode, ErgoTree, EvaluatedValue, FalseLeaf, SigmaPropConstant}
import sigmastate._
import sigmastate.basics.DLogProtocol.ProveDlogProp
import sigmastate.serialization.{ErgoTreeSerializer, SigmaSerializer}

import scala.util.{Failure, Success, Try}

object sigma {

  private val treeSerializer: ErgoTreeSerializer = ErgoTreeSerializer.DefaultSerializer

  @inline def deserializeErgoTree(raw: HexString): Try[Values.ErgoTree] =
    Base16.decode(raw.unwrapped).map(treeSerializer.deserializeErgoTree)

  @inline def deriveErgoTreeTemplateHash(ergoTree: HexString): Try[ErgoTreeTemplateHash] =
    deserializeErgoTree(ergoTree).map { tree =>
      ErgoTreeTemplateHash.fromStringUnsafe(Base16.encode(Sha256.hash(tree.template)))
    }

  @inline def ergoTreeToAddress(
    ergoTree: HexString
  )(implicit enc: ErgoAddressEncoder): Try[ErgoAddress] =
    Base16
      .decode(ergoTree.unwrapped)
      .flatMap { bytes =>
        enc.fromProposition(treeSerializer.deserializeErgoTree(bytes))
      }
      .transform(_ => Try(Pay2SAddress(FalseLeaf.toSigmaProp): ErgoAddress), Failure(_))

  @inline def addressToErgoTree(
    address: Address
  )(implicit enc: ErgoAddressEncoder): ErgoTree =
    enc
      .fromString(address.unwrapped)
      .map(_.script)
      .get

  @inline def addressToErgoTreeHex(address: Address)(implicit enc: ErgoAddressEncoder): Try[HexString] =
    Try(HexString.fromStringUnsafe(Base16.encode(addressToErgoTree(address).bytes)))

  @inline def addressToErgoTreeNewtype(address: Address)(implicit
    enc: ErgoAddressEncoder
  ): Try[uexplorer.ErgoTree] =
    addressToErgoTreeHex(address).map(uexplorer.ErgoTree(_))

  @inline def hexStringToBytes(s: HexString): Try[Array[Byte]] =
    Base16
      .decode(s.unwrapped)
      .transform(Success(_), e => Failure(Base16DecodingFailed(s, Option(e.getMessage))))

  @inline def bytesToErgoTree(bytes: Array[Byte]): Try[ErgoTree] =
    Try(treeSerializer.deserializeErgoTree(bytes))
      .transform(Success(_), e => Failure(ErgoTreeDeserializationFailed(bytes, Option(e.getMessage))))

  /** Extracts ErgoTree's template (serialized tree with placeholders instead of values)
    * @param ergoTree ErgoTree
    * @return serialized ErgoTree's template
    */
  @inline def ergoTreeTemplateBytes(ergoTree: ErgoTree): Try[Array[Byte]] = {
    val bytes = ergoTree.bytes
    Try {
      val r = SigmaSerializer.startReader(bytes)
      treeSerializer.deserializeHeaderWithTreeBytes(r)._4
    }.transform(Success(_), e => Failure(ErgoTreeDeserializationFailed(bytes, Option(e.getMessage))))
  }

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

}
