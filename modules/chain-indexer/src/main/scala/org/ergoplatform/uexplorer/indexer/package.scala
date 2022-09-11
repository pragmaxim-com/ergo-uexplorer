package org.ergoplatform.uexplorer

import akka.stream.{RestartSettings, Supervision}
import cats.implicits.toBifunctorOps
import com.typesafe.scalalogging.LazyLogging
import eu.timepit.refined.refineV
import eu.timepit.refined.string.HexStringSpec
import io.circe
import io.circe.Decoder
import org.ergoplatform.explorer.constraints.HexStringType
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert
import sttp.client3.ResponseException
import sttp.model.Uri

import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal

package object indexer {

  implicit def uriConfigReader(implicit cr: ConfigReader[String]): ConfigReader[Uri] =
    cr.emap(addr => Uri.parse(addr).leftMap(r => CannotConvert(addr, "Uri", r)))

  implicit val hexStringDecoder: Decoder[HexStringType] =
    Decoder.decodeString.emap(str => refineV[HexStringSpec](str))

  type PeerAddress = String
  type ResponseErr = ResponseException[String, circe.Error]

  object Const {
    val ScyllaKeyspace   = "uexplorer"
    val PreGenesisHeight = 0
    val EpochLength      = 1024
    val FlushHeight      = EpochLength / 2

    val FeeContractAddress =
      "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe"

  }

  object Resiliency extends LazyLogging {
    class StopException(msg: String, cause: Throwable) extends RuntimeException(msg, cause)

    val restartSettings = RestartSettings(
      minBackoff   = 3.seconds,
      maxBackoff   = 30.seconds,
      randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
    ).withMaxRestarts(300, 60.minutes) // limits the amount of restarts to 20 within 5 minutes

    val decider: Supervision.Decider = {
      case ex: StopException =>
        logger.error("Stopping stream due to", ex)
        Supervision.stop
      case NonFatal(ex) =>
        logger.error("Stopping stream due to", ex)
        Supervision.stop
    }
  }
}
