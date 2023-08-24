import sbt.*

object Version {
  lazy val sttp            = "3.8.16"
  lazy val circe           = "0.14.3"
  lazy val enumeratum      = "1.7.0"
  lazy val cassandraDriver = "4.15.0"
  lazy val caliban         = "2.0.1"
  lazy val cql4s           = "0.0.1"
  lazy val ergo            = "5.0.8"
  lazy val janus           = "1.0.0-rc1"
  lazy val zio             = "2.0.15"
}

object Dependencies {

  lazy val scrypto    = "org.scorexfoundation"  %% "scrypto"         % "2.3.0" cross CrossVersion.for3Use2_13
  lazy val scorexUtil = "org.scorexfoundation"  %% "scorex-util"     % "0.2.0" cross CrossVersion.for3Use2_13
  lazy val ergoWallet = "org.ergoplatform"      %% "ergo-wallet"     % Version.ergo cross CrossVersion.for3Use2_13
  lazy val pureConfig = "com.github.pureconfig" %% "pureconfig-core" % "0.17.1"

  lazy val commonsCodec   = "commons-codec"        % "commons-codec"   % "1.15"
  lazy val commonsLogging = "commons-logging"      % "commons-logging" % "1.2"
  lazy val gremlin        = "org.apache.tinkerpop" % "gremlin-driver"  % "3.6.1"

  lazy val janusGraph = List(
    "org.janusgraph" % "janusgraph-driver" % Version.janus,
    "org.janusgraph" % "janusgraph-core"   % Version.janus,
    "org.janusgraph" % "janusgraph-cql"    % Version.janus
  )

  lazy val h2 = "com.h2database" % "h2" % "2.1.214"

  lazy val kryo = "com.esotericsoftware" % "kryo" % "5.5.0"

  def zio(v: String) = Seq(
    "dev.zio"     % s"zio-streams_$v"              % "2.0.15",
    "dev.zio"     % s"zio_$v"                      % Version.zio,
    "dev.zio"     % s"zio-http_$v"                 % "3.0.0-RC2",
    "dev.zio"     % s"zio-http-testkit_$v"         % "3.0.0-RC2",
    "dev.zio"     % s"zio-json_$v"                 % "0.6.0",
    "dev.zio"     % s"zio-json-interop-refined_$v" % "0.6.0",
    "io.getquill" % s"quill-jdbc_$v"               % "4.6.0.1",
    "io.getquill" % s"quill-jdbc-zio_$v"           % "4.6.0.1",
    "dev.zio"     % s"zio-config_$v"               % "4.0.0-RC16",
    "dev.zio"     % s"zio-config-typesafe_$v"      % "4.0.0-RC16",
    "dev.zio"     % s"zio-config-magnolia_$v"      % "4.0.0-RC16",
    "dev.zio"     % s"zio-config-refined_$v"       % "4.0.0-RC16",
    "dev.zio"     % s"zio-logging_$v"              % "2.1.13",
    "nl.vroste"   % s"rezilience_$v"               % "0.9.4",
    "dev.zio"     % s"zio-logging-slf4j2_$v"       % "2.1.13"
  ) ++ zioTest(v)

  lazy val cassandraDb = List(
    "com.datastax.oss" % "java-driver-core"             % Version.cassandraDriver,
    "com.datastax.oss" % "java-driver-query-builder"    % Version.cassandraDriver,
    "io.netty"         % "netty-transport-native-epoll" % "4.1.79.Final" classifier "linux-x86_64"
  )

  val discord4j               = "com.discord4j"              % "discord4j-core"    % "3.2.3"
  val loggingApi              = "org.slf4j"                  % "slf4j-api"         % "2.0.3"
  val logback                 = "ch.qos.logback"             % "logback-classic"   % "1.4.3"
  def scalaLogging(v: String) = "com.typesafe.scala-logging" % s"scala-logging_$v" % "3.9.5"

  def zioTest(v: String) = Seq(
    "dev.zio" % s"zio-test_$v"          % Version.zio % Test,
    "dev.zio" % s"zio-test-sbt_$v"      % Version.zio % Test,
    "dev.zio" % s"zio-test-magnolia_$v" % Version.zio % Test
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
    "com.softwaremill.sttp.client3" % s"zio_$v"   % "3.8.16",
    "com.softwaremill.sttp.client3" % s"circe_$v" % Version.sttp
  )
  def tapir(v: String) = List(
    "com.softwaremill.sttp.tapir" % s"tapir-core_$v"              % "1.7.2",
    "com.softwaremill.sttp.tapir" % s"tapir-zio-http-server_$v"   % "1.7.2",
    "com.softwaremill.sttp.tapir" % s"tapir-swagger-ui-bundle_$v" % "1.7.2",
    "com.softwaremill.sttp.tapir" % s"tapir-zio_$v"               % "1.7.2",
    "com.softwaremill.sttp.tapir" % s"tapir-json-zio_$v"          % "1.7.2"
  )
  def monocle(v: String) =
    Seq(
      "dev.optics" % s"monocle-core_$v"  % "3.1.0",
      "dev.optics" % s"monocle-macro_$v" % "3.1.0"
    )

  lazy val allExclusions =
    Seq(
      ExclusionRule("com.typesafe.scala-logging", "scala-logging_2.13"),
      ExclusionRule("org.scala-lang.modules", "scala-collection-compat_2.13"),
      ExclusionRule("org.scala-lang.modules", "scala-java8-compat_2.13"),
      ExclusionRule("com.typesafe", "ssl-config-core_2.13"),
      ExclusionRule("com.lihaoyi", "sourcecode_2.13"),
      ExclusionRule("com.lihaoyi", "fansi_2.13"),
      ExclusionRule("com.lihaoyi", "pprint_2.13"),
      ExclusionRule("io.suzaku", "boopickle_2.13")
    ) ++ (cats("2.13") ++ circe("2.13") :+ commonsLogging).map { x =>
      ExclusionRule(x.organization, x.name)
    }
}
