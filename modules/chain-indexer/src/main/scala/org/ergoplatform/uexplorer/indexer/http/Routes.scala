package org.ergoplatform.uexplorer.indexer.http

import akka.actor.CoordinatedShutdown
import akka.actor.CoordinatedShutdown.Reason
import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives.*
import org.ergoplatform.uexplorer.indexer.http.Routes.HttpShutdownReason

class Routes(implicit s: ActorSystem[Nothing]) {

  val shutdown =
    path("shutdown") {
      post {
        onSuccess(CoordinatedShutdown.get(s).run(HttpShutdownReason)) { _ =>
          complete(StatusCodes.OK)
        }
      }
    }

}

object Routes {
  case object HttpShutdownReason extends Reason

}
