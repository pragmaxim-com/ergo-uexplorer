package org.ergoplatform.uexplorer

import sttp.model.Uri
import sttp.model.Uri.{EmptyPath, QuerySegment}

import scala.collection.mutable

package object indexer {

  implicit class MapPimp[K, V](underlying: Map[K, V]) {

    def putOrRemove(k: K)(f: Option[V] => Option[V]): Map[K, V] =
      f(underlying.get(k)) match {
        case None    => underlying removed k
        case Some(v) => underlying updated (k, v)
      }

    def adjust(k: K)(f: Option[V] => V): Map[K, V] = underlying.updated(k, f(underlying.get(k)))
  }

  implicit class MutableMapPimp[K, V](underlying: mutable.Map[K, V]) {

    def putOrRemove(k: K)(f: Option[V] => Option[V]): mutable.Map[K, V] =
      f(underlying.get(k)) match {
        case None => underlying -= k
        case Some(v) =>
          underlying.put(k, v)
          underlying
      }

    def adjust(k: K)(f: Option[V] => V): mutable.Map[K, V] = {
      underlying.put(k, f(underlying.get(k)))
      underlying
    }
  }

  class UnexpectedStateError(msg: String, cause: Option[Throwable] = None) extends RuntimeException(msg, cause.orNull)

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
