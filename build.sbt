import Dependencies.*
import java.nio.file.Files
import java.util.stream.Collectors
import scala.jdk.CollectionConverters.*

Global / cancelable := true // Allow cancellation of forked task without killing SBT

lazy val commonSettings = Seq(
  organization := "org.ergoplatform",
  scalaVersion := "3.3.0",
  version := "0.0.1",
  resolvers ++= Resolver.sonatypeOssRepos("public") ++ Resolver.sonatypeOssRepos("snapshots"),
  ThisBuild / evictionErrorLevel := Level.Info,
  excludeDependencies ++= allExclusions,
  Compile / doc / sources := Seq.empty,
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-Ykind-projector",
    "-feature",
    "-language:higherKinds",
    "-language:existentials",
    "-unchecked",
    "-Xfatal-warnings"
  )
)

def getPluginJars(modulesDir: File): List[File] =
  modulesDir.listFiles(file => file.getParentFile.getName == "modules" && file.getName.contains("plugin"))
    .flatMap { pluginDir =>
      Files.walk(pluginDir.toPath)
        .map[File](_.toFile)
        .filter(file => file.isDirectory && file.getName == "stage")
        .flatMap(stageDir => Files.walk(stageDir.toPath).map[File](_.toFile).filter(jarFile => jarFile.getName.endsWith(".jar")))
        .collect(Collectors.toList[File]).asScala
    }.toList

def pluginAssemblySettings(moduleName: String) = Seq(
  assembly / assemblyJarName := s"$moduleName.jar",
  assembly / assemblyMergeStrategy := {
    case "logback.xml" => MergeStrategy.first
    case other if other.contains("module-info.class") => MergeStrategy.discard
    case other if other.contains("io.netty.versions") => MergeStrategy.first
    case PathList("deriving.conf") => MergeStrategy.concat
    case other if other.contains("getquill") => MergeStrategy.last
    case other => (assembly / assemblyMergeStrategy).value(other)
  },
  Universal / mappings := {
    val universalMappings = (Universal / mappings).value
    val fatJar = (Compile / assembly).value
    List(fatJar -> s"lib/${fatJar.getName}")
  },
  scriptClasspath := Seq((assembly / assemblyJarName).value)
)

def chainIndexerAssemblySettings = Seq(
  assembly / assemblyJarName := "chain-indexer.jar",
  assembly / assemblyMergeStrategy := {
    case "logback.xml" => MergeStrategy.first
    case other if other.contains("module-info.class") => MergeStrategy.discard
    case other if other.contains("ExtensionModule") => MergeStrategy.first
    case other if other.contains("getquill") => MergeStrategy.last
    case PathList("deriving.conf") => MergeStrategy.concat
    case other if other.contains(".proto") => MergeStrategy.first
    case other if other.contains("io.netty.versions") => MergeStrategy.first
    case other => (assembly / assemblyMergeStrategy).value(other)
  },
  Universal / mappings := {
    val universalMappings = (Universal / mappings).value
    val fatJar = (Compile / assembly).value
    val jars = (fatJar :: getPluginJars(baseDirectory.value.getParentFile)).map(jar => jar -> s"lib/${jar.getName}")
    val resources = universalMappings filter {
      case (file, name) => !name.endsWith(".jar")
    }
    val configs = (baseDirectory.value / ".." / ".." / "docker" / "chain-indexer.conf" get).map(x => x -> s"conf/${x.getName}")
    resources ++ jars ++ configs
  },
  scriptClasspath := Seq((assembly / assemblyJarName).value),
  Universal / javaOptions ++= Seq(
    "-J-Xmx16G",
    "-J-Xms512m",
    "-J-Xss40m",
  )
)

lazy val root = (project in file("."))
  .settings(
    name := "ergo-uexplorer"
  ).aggregate(core, `node-pool`, `alert-plugin`, backend, mvstore, storage, indexer)// TODO return `cassandra, janusgraph` and org.ergoplatform.uexplorer.plugin.alert.AlertPlugin to META-INF.services

lazy val core =
  Utils.mkModule("explorer-core", "explorer-core")
    .settings(commonSettings)
    .settings(libraryDependencies ++= zio("3") ++ circe("3") ++ monocle("3") ++ refined("3") ++ Seq(retry("3"), pureConfig, ergoWallet, gremlin, loggingApi, scalaLogging("3")))

lazy val `node-pool` =
  Utils.mkModule("node-pool", "node-pool")
    .settings(commonSettings)
    .settings(testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"))
    .settings(libraryDependencies ++= zio("3") ++ sttp("3"))
    .dependsOn(core % "compile->compile;test->test")

lazy val mvstore =
  Utils.mkModule("mvstore", "mvstore")
    .settings(commonSettings)
    .settings(libraryDependencies ++= zio("3") ++ Seq(h2, loggingApi, scalaLogging("3")) ++ scalatest("3"))

lazy val storage =
  Utils.mkModule("storage", "storage")
    .settings(commonSettings)
    .settings(testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"))
    .settings(libraryDependencies ++= zio("3") ++ Seq(kryo) ++ scalatest("3"))
    .dependsOn(mvstore, core % "compile->compile;test->test")

lazy val cassandra =
  Utils.mkModule("cassandra", "cassandra")
    .settings(commonSettings)
    .settings(libraryDependencies ++= zio("3") ++ cassandraDb ++ Seq(commonsCodec))
    .dependsOn(core)

lazy val janusgraph =
  Utils.mkModule("janusgraph", "janusgraph")
    .settings(commonSettings)
    .settings(libraryDependencies ++= zio("3") ++ janusGraph ++ cassandraDb ++ scalatest("3"))
    .dependsOn(core)

lazy val `alert-plugin` =
  Utils.mkModule("alert-plugin", "alert-plugin")
    .enablePlugins(JavaAppPackaging)
    .settings(commonSettings)
    .settings(pluginAssemblySettings("alert-plugin"))
    .settings(libraryDependencies ++= scalatest("3") ++ Seq(discord4j, logback))
    .dependsOn(core)

lazy val backend =
  Utils.mkModule("backend", "backend")
    .settings(commonSettings)
    .settings(testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"))
    .settings(libraryDependencies ++= zio("3") ++ tapir("3") ++ Seq(h2) ++ scalatest("3"))
    .dependsOn(core % "compile->compile;test->test", `node-pool` % "compile->compile;test->test")

lazy val indexer =
  Utils.mkModule("chain-indexer", "chain-indexer")
    .enablePlugins(JavaAppPackaging)
    .settings(commonSettings)
    .settings(chainIndexerAssemblySettings)
    .settings(testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"))
    .settings(libraryDependencies ++= Seq(logback) ++ zio("3") ++ scalatest("3"))
    .dependsOn(core  % "compile->compile;test->test", `node-pool` % "compile->compile;test->test", backend, storage) //todo return cassandra and janusgraph
