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
  excludeDependencies ++= allExclusions.map( x => ExclusionRule(x.organization, x.name)),
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding",
    "UTF-8",
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
    "-J-Xmx10G",
    "-J-Xms512m",
    "-J-Xss40m",
  )
)

lazy val root = (project in file("."))
  .settings(
    name := "ergo-uexplorer"
  ).aggregate(core, `node-pool`, backend, mvstore, storage, cassandra, janusgraph, indexer)// TODO return `alert-plugin` and org.ergoplatform.uexplorer.plugin.alert.AlertPlugin to META-INF.services

lazy val core =
  Utils.mkModule("explorer-core", "explorer-core")
    .settings(commonSettings)
    .settings(libraryDependencies ++= akkaStream("3") ++ circe("3") ++ monocle("3") ++ refined("3") ++ Seq(retry("3"), pureConfig, ergoWallet, gremlin, loggingApi, scalaLogging("3")))

lazy val `node-pool` =
  Utils.mkModule("node-pool", "node-pool")
    .settings(commonSettings)
    .settings(libraryDependencies ++= akkaStream("3") ++ sttp("3") ++ scalatest("3"))
    .dependsOn(core)

lazy val mvstore =
  Utils.mkModule("mvstore", "mvstore")
    .settings(commonSettings)
    .settings(libraryDependencies ++= Seq(h2, loggingApi, scalaLogging("3")) ++ scalatest("3"))

lazy val storage =
  Utils.mkModule("storage", "storage")
    .settings(commonSettings)
    .settings(libraryDependencies ++= akkaStream("3") ++ Seq(kryo) ++ scalatest("3"))
    .dependsOn(mvstore, core)

lazy val cassandra =
  Utils.mkModule("cassandra", "cassandra")
    .settings(commonSettings)
    .settings(libraryDependencies ++= akkaStream("3") ++ cassandraDb ++ Seq(commonsCodec))
    .dependsOn(core)

lazy val janusgraph =
  Utils.mkModule("janusgraph", "janusgraph")
    .settings(commonSettings)
    .settings(libraryDependencies ++= akkaStream("3") ++ janusGraph ++ cassandraDb ++ scalatest("3"))
    .dependsOn(core)

lazy val `alert-plugin` =
  Utils.mkModule("alert-plugin", "alert-plugin")
    .enablePlugins(JavaAppPackaging)
    .settings(commonSettings)
    .settings(pluginAssemblySettings("alert-plugin"))
    .settings(libraryDependencies ++= scalatest("3") ++ Seq(discord4j, logback))
    .settings(excludeDependencies += ExclusionRule(commonsLogging.organization, commonsLogging.name))
    .dependsOn(core)

lazy val backend =
  Utils.mkModule("backend", "backend")
    .settings(commonSettings)
    .settings(testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"))
    .settings(libraryDependencies ++= akkaStream("3") ++ zio("3") ++ Seq(h2) ++ scalatest("3"))
    .settings(excludeDependencies ++= Seq(ExclusionRule("com.lihaoyi", "sourcecode_2.13"), ExclusionRule("com.lihaoyi", "fansi_2.13"), ExclusionRule("com.lihaoyi", "pprint_2.13"), ExclusionRule("io.suzaku", "boopickle_2.13")))
    .dependsOn(core)

lazy val indexer =
  Utils.mkModule("chain-indexer", "chain-indexer")
    .enablePlugins(JavaAppPackaging)
    .settings(commonSettings)
    .settings(chainIndexerAssemblySettings)
    .settings(libraryDependencies ++= Seq(logback, akkaHttp("3")) ++ akkaStream("3") ++ scalatest("3"))
    .settings(excludeDependencies ++= Seq(ExclusionRule("com.lihaoyi", "sourcecode_2.13"), ExclusionRule("com.lihaoyi", "fansi_2.13"), ExclusionRule("com.lihaoyi", "pprint_2.13"), ExclusionRule("io.suzaku", "boopickle_2.13")) ++ cats("2.13").map( x => ExclusionRule(x.organization, x.name)) ++ circe("2.13").map( x => ExclusionRule(x.organization, x.name)) ++ Seq(ExclusionRule(commonsLogging.organization, commonsLogging.name)))
    .dependsOn(core, `node-pool` % "compile->compile;test->test", backend, storage, cassandra, janusgraph)
