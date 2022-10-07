package org.ergoplatform.uexplorer

import sttp.model.Uri
import sttp.model.Uri.{EmptyPath, QuerySegment}

package object indexer {

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
