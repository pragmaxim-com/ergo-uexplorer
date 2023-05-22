package org.ergoplatform.uexplorer

import akka.stream.{RestartSettings, SharedKillSwitch, Supervision}
import com.typesafe.scalalogging.LazyLogging
import retry.Policy
import sttp.model.Uri

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object Resiliency extends LazyLogging {

  val restartSettings: RestartSettings = RestartSettings(
    minBackoff   = 3.seconds,
    maxBackoff   = 30.seconds,
    randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
  ).withMaxRestarts(300, 60.minutes) // limits the amount of restarts to 20 within 5 minutes

  def decider: Supervision.Decider = {
    case ex: UnexpectedStateError =>
      logger.error("Stopping stream due to", ex)
      Supervision.stop
    case NonFatal(ex) =>
      logger.error("Stopping stream due to", ex)
      Supervision.stop
  }
}
