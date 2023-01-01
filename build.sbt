import Dependencies._
import java.nio.file.Files
import java.util.stream.Collectors
import scala.jdk.CollectionConverters._

Global / cancelable := true // Allow cancellation of forked task without killing SBT

lazy val commonSettings = Seq(
  organization := "org.ergoplatform",
  scalaVersion := "3.2.0",
  version := "0.0.1",
  resolvers ++= Resolver.sonatypeOssRepos("public") ++ Resolver.sonatypeOssRepos("snapshots"),
  ThisBuild / evictionErrorLevel := Level.Info,
  excludeDependencies ++= allExclusions.map( x => ExclusionRule(x.organization, x.name)),
  scalacOptions ++= Seq(
    "-Xmax-inlines", "512",
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
    "-J-Xmx4G",
    "-J-Xms512m",
    "-J-Xss40m",
  )
)

lazy val root = (project in file("."))
  .settings(
    name := "ergo-uexplorer"
  ).aggregate(core, `alert-plugin`, indexer)

lazy val core =
  Utils.mkModule("explorer-core", "explorer-core")
    .settings(commonSettings)
    .settings(libraryDependencies ++= circe("3") ++ Seq(gremlin))

lazy val `alert-plugin` =
  Utils.mkModule("alert-plugin", "alert-plugin")
    .enablePlugins(JavaAppPackaging)
    .settings(commonSettings)
    .settings(pluginAssemblySettings("alert-plugin"))
    .settings(libraryDependencies ++= scalatest("3") ++ Seq(retry("3"), gremlin, discord4j, loggingApi, logback))
    .settings(excludeDependencies += ExclusionRule(commonsLogging.organization, commonsLogging.name))
    .dependsOn(core)

lazy val indexer =
  Utils.mkModule("chain-indexer", "chain-indexer")
    .enablePlugins(JavaAppPackaging)
    .settings(commonSettings)
    .settings(chainIndexerAssemblySettings)
    .settings(libraryDependencies ++= lightBend("3") ++ sttp("3") ++ cassandraDb ++ monocle("3") ++ refined("3") ++ scalatest("3") ++ janusGraph ++ Seq(gremlin, retry("3"), commonsCodec, ergoWallet, loggingApi, logback, pureConfig))
    .settings(excludeDependencies ++= cats("2.13").map( x => ExclusionRule(x.organization, x.name)) ++ circe("2.13").map( x => ExclusionRule(x.organization, x.name)) ++ Seq(ExclusionRule(commonsLogging.organization, commonsLogging.name)))
    .dependsOn(core, `alert-plugin`)
