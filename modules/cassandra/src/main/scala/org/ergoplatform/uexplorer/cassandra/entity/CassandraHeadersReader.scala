package org.ergoplatform.uexplorer.cassandra.entity

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.oss.driver.api.core.cql.*
import com.datastax.oss.driver.api.core.data.TupleValue
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.typesafe.scalalogging.LazyLogging
import eu.timepit.refined.auto.*
import org.ergoplatform.uexplorer.{MapPimp, MutableMapPimp}
import org.ergoplatform.uexplorer.cassandra.{CassandraBackend, CassandraPersistenceSupport}
import org.ergoplatform.uexplorer.db
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId}
import org.ergoplatform.uexplorer.cassandra
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.{ArraySeq, TreeMap, TreeSet}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.FutureConverters.*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success}
import org.ergoplatform.uexplorer.BlockMetadata

trait CassandraHeadersReader extends LazyLogging {
  this: CassandraBackend =>

  import CassandraHeadersReader.*

  private lazy val headerSelectWhereHeader = cqlSession.prepare(headerIdSelectStatement)

  def isEmpty: Boolean = !cqlSession.execute(headerSelectWhereHeader.bind()).iterator().hasNext
}

object CassandraHeadersReader extends CassandraPersistenceSupport {

  protected[cassandra] val headerIdSelectStatement: SimpleStatement =
    QueryBuilder
      .selectFrom(cassandra.Const.CassandraKeyspace, Headers.node_headers_table)
      .columns(Headers.header_id)
      .limit(1)
      .build()

}
