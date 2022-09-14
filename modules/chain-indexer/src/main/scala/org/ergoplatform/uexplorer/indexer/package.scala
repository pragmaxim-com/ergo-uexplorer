package org.ergoplatform.uexplorer

import cats.implicits.toBifunctorOps
import eu.timepit.refined.refineV
import eu.timepit.refined.string.HexStringSpec
import io.circe.Decoder
import org.ergoplatform.explorer.constraints.HexStringType
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert
import sttp.model.Uri
import sttp.model.Uri.{EmptyPath, QuerySegment}

package object indexer {

  implicit def uriConfigReader(implicit cr: ConfigReader[String]): ConfigReader[Uri] =
    cr.emap(addr => Uri.parse(addr).leftMap(r => CannotConvert(addr, "Uri", r)))

  implicit val hexStringDecoder: Decoder[HexStringType] =
    Decoder.decodeString.emap(str => refineV[HexStringSpec](str))

  type PeerAddress = String

  class StopException(msg: String, cause: Throwable) extends RuntimeException(msg, cause)

  object Const {
    val ScyllaKeyspace    = "uexplorer"
    val PreGenesisHeight  = 0
    val EpochLength       = 1024
    val FlushHeight       = EpochLength / 2
    val AllowedHeightDiff = EpochLength / 2
    val MinNodeHeight     = EpochLength * 800

    val FeeContractAddress =
      "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe"

  }

  object Utils {

    def copyUri(origUri: Uri, newUri: Uri): Uri =
      newUri.copy(
        pathSegments    = origUri.pathSegments,
        querySegments   = origUri.querySegments,
        fragmentSegment = origUri.fragmentSegment
      )

    def stripUri(uri: Uri): Uri =
      uri.copy(
        pathSegments    = EmptyPath,
        querySegments   = List.empty[QuerySegment],
        fragmentSegment = Option.empty
      )
  }
}
