package org.ergoplatform.uexplorer

class UnexpectedStateError(msg: String, cause: Option[Throwable] = None) extends RuntimeException(msg, cause.orNull)