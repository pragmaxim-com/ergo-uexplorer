package org.ergoplatform.uexplorer.graphql

import ExampleData._
import cql4s.{CassandraConfig, CassandraZIORuntime, CassandraZLayer}
import zio.ZIOAppDefault
import caliban.ZHttpAdapter
import zio._
import zio.stream._
import zhttp.http._
import zhttp.service.Server


object Application extends ZIOAppDefault:

/*
  val cassandraLayer = CassandraZLayer(CassandraConfig(
    "localhost",
    9044,
    credentials = None,
    keyspace = None,
    datacenter = "datacenter1"
  ))

  val dbQuery =
    new HeadersRepo(using CassandraZIORuntime)
        .findById("b0244dfc267baca974a4caee06120321562784303a8a688976ae56170e4d175b").tap(e => ZIO.attempt(println(e)))
        .provideSomeLayer(cassandraLayer)
*/

  private val graphiql = Http.fromStream(ZStream.fromResource("graphiql.html"))

  override def run: URIO[Any, ExitCode] =
    (for {
      interpreter <- ExampleApi.api.interpreter
      _ <- Server
        .start(
          8088,
          Http.collectHttp[Request] {
            case _ -> !! / "api" / "graphql" => ZHttpAdapter.makeHttpService(interpreter)
            case _ -> !! / "ws" / "graphql" => ZHttpAdapter.makeWebSocketService(interpreter)
            case _ -> !! / "graphiql" => graphiql
          }
        )
        .forever
    } yield ()).provideLayer(ExampleService.make(ExampleData.sampleCharacters)).exitCode

