package org.ergoplatform.uexplorer.indexer

import akka.stream.{RestartSettings, SharedKillSwitch, Supervision}
import com.typesafe.scalalogging.LazyLogging
import retry.Policy
import sttp.model.Uri

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

trait ResiliencySupport extends LazyLogging {

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

}

object Resiliency extends LazyLogging {

  val restartSettings: RestartSettings = RestartSettings(
    minBackoff   = 3.seconds,
    maxBackoff   = 30.seconds,
    randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
  ).withMaxRestarts(300, 60.minutes) // limits the amount of restarts to 20 within 5 minutes

  def decider(killSwitch: SharedKillSwitch): Supervision.Decider = {
    case ex: UnexpectedStateError =>
      logger.error("Stopping stream due to", ex)
      killSwitch.shutdown()
      Supervision.stop
    case NonFatal(ex) =>
      logger.error("Stopping stream due to", ex)
      killSwitch.shutdown()
      Supervision.stop
  }
}
