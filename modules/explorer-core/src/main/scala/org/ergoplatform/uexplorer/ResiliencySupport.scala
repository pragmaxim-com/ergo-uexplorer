package org.ergoplatform.uexplorer

import com.typesafe.scalalogging.LazyLogging
import retry.Policy

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

trait ResiliencySupport extends LazyLogging {

  def fallback[T, R](uris: List[T], retryPolicy: Policy)(req: T => Future[R]): Future[R] = {
    def recursiveRetry(uris: List[T]): Future[R] =
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
