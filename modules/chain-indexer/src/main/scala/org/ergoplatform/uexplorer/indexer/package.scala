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
import retry.Policy
import sttp.client3.ResponseException
import sttp.model.Uri

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Random, Success}
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

  object Resiliency extends LazyLogging {
    class StopException(msg: String, cause: Throwable) extends RuntimeException(msg, cause)

    def fallback[R](uris: List[Uri], retryPolicy: Policy)(req: Uri => Future[R]): Future[R] = {
      def recursiveRetry(uris: List[Uri]): Future[R] =
        uris match {
          case head :: tail =>
            req(head).transformWith {
              case Success(result) =>
                Future.successful(result)
              case Failure(ex) =>
                tail.headOption.foreach { next =>
                  logger.warn(s"$head failed, retrying with another $next", ex)
                }
                recursiveRetry(tail)
            }
          case Nil =>
            Future.failed(new Exception(s"None of ${uris.mkString(", ")} succeeded"))
        }

      retryPolicy
        .apply { () =>
          recursiveRetry(uris)
        }(retry.Success.always, global)
    }

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
