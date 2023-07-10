package org.ergoplatform.uexplorer.cassandra.entity

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.oss.driver.api.core.cql.*
import com.datastax.oss.driver.api.core.data.TupleValue
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.typesafe.scalalogging.LazyLogging
import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.cassandra.{CassandraBackend, CassandraPersistenceSupport}
import org.ergoplatform.uexplorer.{cassandra, BlockId, BoxId, ErgoTreeHex, MapPimp, MutableMapPimp}

import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.{ArraySeq, TreeMap, TreeSet}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*
import scala.util.{Failure, Success}

trait CassandraHeadersReader extends LazyLogging {
  this: CassandraBackend =>

  import CassandraHeadersReader.*

  private lazy val headerSelectWhereHeader = cqlSession.prepare(headerIdSelectStatement)

  def isEmpty: Future[Boolean] =
    cqlSession.executeAsync(headerSelectWhereHeader.bind()).asScala.map(r => !r.currentPage().iterator().hasNext)
}

object CassandraHeadersReader extends CassandraPersistenceSupport {

  protected[cassandra] val headerIdSelectStatement: SimpleStatement =
    QueryBuilder
      .selectFrom(cassandra.Const.CassandraKeyspace, Headers.node_headers_table)
      .columns(Headers.header_id)
      .limit(1)
      .build()

}
