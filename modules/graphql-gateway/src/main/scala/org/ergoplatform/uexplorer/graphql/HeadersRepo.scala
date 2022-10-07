package org.ergoplatform.uexplorer.graphql

import com.datastax.oss.driver.api.core.ConsistencyLevel
import cql4s.CassandraRuntimeAlgebra
import cql4s.dsl.*

import scala.util.chaining.*

class HeadersRepo[F[_], S[_]](using CassandraRuntimeAlgebra[F, S]):
  val findById: String => F[Header] =
    Select
      .from(headers)
      .take(_.*)
      .where(_("header_id") === :?)
      .compile
      .pmap[Header]
      .one

