package org.ergoplatform.uexplorer

import java.util.regex.Pattern
import org.ergoplatform.uexplorer.node.TokenProps
import sigmastate.{SByte, SCollection}
import tofu.syntax.monadic._

import scala.util.Try

object TokenPropsParser {

  private val StringCharset = "UTF-8"
  private val MaxStringLen  = 1000

  def parse(registers: Map[RegisterId, HexString]): Option[TokenProps] = {
    def parse(raw: HexString) = RegistersParser.parse[SCollection[SByte.type]](raw).toOption

    val r4      = registers.get(RegisterId.R4)
    val r5      = registers.get(RegisterId.R5)
    val r6      = registers.get(RegisterId.R6)
    val nameRaw = r4 >>= parse
    val nameOpt = nameRaw >>= (raw => toUtf8String(raw.toArray))
    nameOpt.map { name =>
      val descriptionRaw = r5 >>= parse
      val decimalsRaw    = r6 >>= parse
      val descriptionOpt = descriptionRaw >>= (raw => toUtf8String(raw.toArray))
      val decimalsOpt =
        Try(decimalsRaw >>= (raw => toUtf8String(raw.toArray).map(_.toInt))).toOption.flatten
      val (description, decimals) = (descriptionOpt.getOrElse(""), decimalsOpt.getOrElse(0))
      TokenProps(name, description, decimals)
    }
  }

  private def looksLikeUTF8(utf8: Array[Byte]): Boolean = {
    val p = Pattern.compile(
      "\\A(\n" +
      "  [\\x09\\x0A\\x0D\\x20-\\x7E]             # ASCII\\n" +
      "| [\\xC2-\\xDF][\\x80-\\xBF]               # non-overlong 2-byte\n" +
      "|  \\xE0[\\xA0-\\xBF][\\x80-\\xBF]         # excluding overlongs\n" +
      "| [\\xE1-\\xEC\\xEE\\xEF][\\x80-\\xBF]{2}  # straight 3-byte\n" +
      "|  \\xED[\\x80-\\x9F][\\x80-\\xBF]         # excluding surrogates\n" +
      "|  \\xF0[\\x90-\\xBF][\\x80-\\xBF]{2}      # planes 1-3\n" +
      "| [\\xF1-\\xF3][\\x80-\\xBF]{3}            # planes 4-15\n" +
      "|  \\xF4[\\x80-\\x8F][\\x80-\\xBF]{2}      # plane 16\n" +
      ")*\\z",
      Pattern.COMMENTS
    )
    val phonyString = new String(utf8, "ISO-8859-1")
    p.matcher(phonyString).matches
  }

  private def toUtf8String(raw: Array[Byte]): Option[String] =
    if (raw.length <= MaxStringLen && looksLikeUTF8(raw))
      Try(new String(raw, StringCharset)).toOption
    else None
}
