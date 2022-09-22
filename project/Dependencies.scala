import sbt._

object Version {
  lazy val akka            = "2.6.19"
  lazy val sttp            = "3.7.6"
  lazy val circe           = "0.14.2"
  lazy val enumeratum      = "1.7.0"
  lazy val alpakka         = "3.0.4"
  lazy val cassandraDriver = "4.14.1"
}

object Dependencies {
  lazy val akkaActor           = "com.typesafe.akka"          %% "akka-actor-typed"          % Version.akka
  lazy val akkaStream          = "com.typesafe.akka"          %% "akka-stream-typed"         % Version.akka
  lazy val akkaTest            = "com.typesafe.akka"          %% "akka-actor-testkit-typed"  % Version.akka % Test
  lazy val akkaSlf4j           = "com.typesafe.akka"          %% "akka-slf4j"                % Version.akka
  lazy val scalaTest           = "org.scalatest"              %% "scalatest"                 % "3.2.12"     % Test
  lazy val scalaCheck          = "org.scalatestplus"          %% "scalacheck-1-15"           % "3.2.11.0"   % Test
  lazy val scalaCheckShapeless = "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % "1.2.5"      % Test
  lazy val scalaLogging        = "com.typesafe.scala-logging" %% "scala-logging"             % "3.9.5"
  lazy val slf4jApi            = "org.slf4j"                   % "slf4j-api"                 % "1.7.36"
  lazy val logback             = "ch.qos.logback"              % "logback-classic"           % "1.2.11"
  lazy val refined             = "eu.timepit"                 %% "refined"                   % "0.9.29"
  lazy val derevo              = "org.manatki"                %% "derevo-circe"              % "0.11.6"
  lazy val explorerGrabber     = "org.ergoplatform"           %% "chain-grabber"             % "9.17.3"
  lazy val pureConfig          = "com.github.pureconfig"      %% "pureconfig"                % "0.17.1"
  lazy val diffx               = "com.softwaremill.diffx"     %% "diffx-scalatest-should"    % "0.7.1"      % Test

  lazy val logging = Seq(slf4jApi, logback, scalaLogging)
  lazy val akka    = Seq(akkaActor, akkaStream, akkaSlf4j, akkaTest)

  lazy val circe = List(
    "io.circe" %% "circe-core"    % Version.circe,
    "io.circe" %% "circe-generic" % Version.circe,
    "io.circe" %% "circe-parser"  % Version.circe
  )

  val enumeratums: List[ModuleID] = List(
    "com.beachape" %% "enumeratum"       % Version.enumeratum,
    "com.beachape" %% "enumeratum-circe" % Version.enumeratum
  )

  val sttp = List(
    "com.softwaremill.sttp.client3" %% "core"              % Version.sttp,
    "com.softwaremill.sttp.client3" %% "circe"             % Version.sttp,
    "com.softwaremill.sttp.client3" %% "akka-http-backend" % Version.sttp,
    "com.softwaremill.retry"        %% "retry"             % "0.3.5"
  )

  val cassandraDb = List(
    "com.lightbend.akka" %% "akka-stream-alpakka-cassandra" % Version.alpakka,
    "com.datastax.oss"    % "java-driver-core"              % Version.cassandraDriver,
    "com.datastax.oss"    % "java-driver-query-builder"     % Version.cassandraDriver,
    "io.netty"            % "netty-transport-native-epoll"  % "4.1.79.Final" classifier "linux-x86_64"
  )

  val monocle =
    Seq(
      "com.github.julien-truffaut" %% "monocle-core"  % "2.0.5",
      "com.github.julien-truffaut" %% "monocle-macro" % "2.0.5"
    )
}
