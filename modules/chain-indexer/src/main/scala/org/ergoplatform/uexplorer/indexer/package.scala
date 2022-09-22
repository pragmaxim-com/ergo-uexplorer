package org.ergoplatform.uexplorer

import cats.implicits.toBifunctorOps
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert
import sttp.model.Uri
import sttp.model.Uri.{EmptyPath, QuerySegment}

package object indexer {

  implicit def uriConfigReader(implicit cr: ConfigReader[String]): ConfigReader[Uri] =
    cr.emap(addr => Uri.parse(addr).leftMap(r => CannotConvert(addr, "Uri", r)))

  class UnexpectedStateError(msg: String, cause: Option[Throwable] = None) extends RuntimeException(msg, cause.orNull)

  object Const {
    val CassandraKeyspace = "uexplorer"
    val EpochLength       = 1024
    val BufferSize        = 32
    val FlushHeight       = 32
    val AllowedHeightDiff = 10
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
