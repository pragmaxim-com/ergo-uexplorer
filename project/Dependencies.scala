import sbt._

object Version {
  lazy val akka            = "2.7.0"
  lazy val sttp            = "3.8.2"
  lazy val circe           = "0.14.3"
  lazy val enumeratum      = "1.7.0"
  lazy val cassandraDriver = "4.15.0"
  lazy val caliban         = "2.0.1"
  lazy val cql4s           = "0.0.1"
  lazy val ergo            = "4.0.42-153-3b68716c-20221008-0749-SNAPSHOT"
}

object Dependencies {

  val scorexUtil = "org.scorexfoundation"  %% "scorex-util"     % "0.1.8" cross CrossVersion.for3Use2_13
  val ergoWallet = "org.ergoplatform"      %% "ergo-wallet"     % Version.ergo cross CrossVersion.for3Use2_13
  val pureConfig = "com.github.pureconfig" %% "pureconfig-core" % "0.17.1"

  lazy val commonsCodec   = "commons-codec"           % "commons-codec"     % "1.15"
  lazy val commonsLogging = "commons-logging"         % "commons-logging"   % "1.2"
  lazy val gremlin        = "org.apache.tinkerpop"    % "gremlin-driver"    % "3.6.1"
  lazy val sketches       = "org.apache.datasketches" % "datasketches-java" % "3.1.0"

  lazy val janusGraph = List(
    "org.janusgraph" % "janusgraph-driver" % "1.0.0-rc1",
    "org.janusgraph" % "janusgraph-core"   % "1.0.0-rc1",
    "org.janusgraph" % "janusgraph-cql"    % "1.0.0-rc1"
  )

  lazy val cassandraDb = List(
    "com.datastax.oss" % "java-driver-core"             % Version.cassandraDriver,
    "com.datastax.oss" % "java-driver-query-builder"    % Version.cassandraDriver,
    "io.netty"         % "netty-transport-native-epoll" % "4.1.79.Final" classifier "linux-x86_64"
  )

  val discord4j  = "com.discord4j"  % "discord4j-core"  % "3.2.3"
  val loggingApi = "org.slf4j"      % "slf4j-api"       % "2.0.3"
  val logback    = "ch.qos.logback" % "logback-classic" % "1.4.3"

  def lightBend(v: String) = Seq(
    "com.typesafe.akka"          % s"akka-actor_$v"               % Version.akka,
    "com.typesafe.akka"          % s"akka-http_$v"                % "10.5.0-M1",
    "com.typesafe.akka"          % s"akka-actor-typed_$v"         % Version.akka,
    "com.typesafe.akka"          % s"akka-stream-typed_$v"        % Version.akka,
    "com.typesafe.akka"          % s"akka-actor-testkit-typed_$v" % Version.akka % Test,
    "com.typesafe.akka"          % s"akka-slf4j_$v"               % Version.akka,
    "com.typesafe.scala-logging" % s"scala-logging_$v"            % "3.9.5"
  )

  def scalatest(v: String) = Seq(
    "com.softwaremill.diffx" % s"diffx-scalatest-should_$v" % "0.7.1"    % Test,
    "org.scalatest"          % s"scalatest_$v"              % "3.2.14"   % Test,
    "org.scalatestplus"      % s"scalacheck-1-15_$v"        % "3.2.11.0" % Test
  )

  def circe(v: String) = List(
    "io.circe" % s"circe-core_$v"    % Version.circe,
    "io.circe" % s"circe-generic_$v" % Version.circe,
    "io.circe" % s"circe-parser_$v"  % Version.circe,
    "io.circe" % s"circe-refined_$v" % Version.circe
  )

  def cats(v: String) = List(
    "org.typelevel" % s"cats-kernel_$v" % "2.8.0",
    "org.typelevel" % s"cats-core_$v"   % "2.8.0"
  )

  def refinedPureConfig = "eu.timepit" %% s"refined-pureconfig" % "0.10.1" cross CrossVersion.for3Use2_13

  def refined(v: String): List[ModuleID] = List(
    "eu.timepit" % s"refined_$v"      % "0.10.1",
    "eu.timepit" % s"refined-cats_$v" % "0.10.1"
  )

  def retry(v: String) = "com.softwaremill.retry" % s"retry_$v" % "0.3.6"

  def sttp(v: String) = List(
    "com.softwaremill.sttp.client3" % s"core_$v"  % Version.sttp,
    "com.softwaremill.sttp.client3" % s"circe_$v" % Version.sttp
  )

  def monocle(v: String) =
    Seq(
      "dev.optics" % s"monocle-core_$v"  % "3.1.0",
      "dev.optics" % s"monocle-macro_$v" % "3.1.0"
    )

  def allExclusions = cats("2.13") ++ circe("2.13") ++ lightBend("2.13") ++ Seq(
    "com.typesafe.akka"      % "akka-actor_2.13"              % "any",
    "com.typesafe.akka"      % "akka-stream_2.13"             % "any",
    "com.typesafe.akka"      % "akka-protobuf-v3_2.13"        % "any",
    "org.scala-lang.modules" % "scala-collection-compat_2.13" % "any",
    "org.scala-lang.modules" % "scala-java8-compat_2.13"      % "any",
    "com.typesafe"           % "ssl-config-core_2.13"         % "any"
  )
}
