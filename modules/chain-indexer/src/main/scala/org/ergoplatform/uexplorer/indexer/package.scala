package org.ergoplatform.uexplorer

import cats.implicits.toBifunctorOps
import eu.timepit.refined.refineV
import eu.timepit.refined.string.HexStringSpec
import io.circe
import io.circe.Decoder
import org.ergoplatform.explorer.constraints.HexStringType
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert
import sttp.client3.ResponseException
import sttp.model.Uri

import scala.util.Random

package object indexer {

  implicit def uriConfigReader(implicit cr: ConfigReader[String]): ConfigReader[Uri] =
    cr.emap(addr => Uri.parse(addr).leftMap(r => CannotConvert(addr, "Uri", r)))

  implicit val hexStringDecoder: Decoder[HexStringType] =
    Decoder.decodeString.emap(str => refineV[HexStringSpec](str))

  type PeerAddress = String
  type ResponseErr = ResponseException[String, circe.Error]

  class StopException(msg: String, cause: Throwable) extends RuntimeException(msg, cause)

  object Const {
    val ScyllaKeyspace   = "uexplorer"
    val PreGenesisHeight = 0
    val EpochLength      = 1024
    val FlushHeight      = EpochLength / 2

    val FeeContractAddress =
      "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe"

  }

  object Utils {

    def shuffle[T](arr: Array[T]): Vector[T] = {
      def swap(i1: Int, i2: Int): Unit = {
        val tmp = arr(i1)
        arr(i1) = arr(i2)
        arr(i2) = tmp
      }

      for (n <- arr.length to 2 by -1) {
        val k = Random.nextInt(n)
        swap(n - 1, k)
      }

      arr.toVector
    }

  }
}
