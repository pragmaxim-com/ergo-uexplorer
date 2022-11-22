import Dependencies._

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

def assemblySettings(moduleName: String) = Seq(
  assembly / assemblyJarName := s"$moduleName.jar",
  assembly / assemblyMergeStrategy := {
    case "logback.xml" => MergeStrategy.first
    case other if other.contains("module-info.class") => MergeStrategy.discard
    case other if other.contains(".proto") => MergeStrategy.first
    case other if other.contains("io.netty.versions") => MergeStrategy.first
    case other => (assembly / assemblyMergeStrategy).value(other)
  },
  Universal / mappings ++= (baseDirectory.value / ".." / ".." / "docker" / s"$moduleName.conf" get) map (x => x -> ("conf/" + x.getName)),
  Universal / mappings := {
    val universalMappings = (Universal / mappings).value
    val fatJar = (Compile / assembly).value
    val filtered = universalMappings filter {
      case (file, name) => !name.endsWith(".jar")
    }
    filtered :+ (fatJar -> ("lib/" + fatJar.getName))
  },
  scriptClasspath := Seq((assembly / assemblyJarName).value),
  Universal / javaOptions ++= Seq(
    "-J-Xmx2G",
    "-J-Xms512m",
    "-J-Xss40m",
  )
)

lazy val root = (project in file("."))
  .settings(
    name := "ergo-uexplorer"
  ).aggregate(core, indexer)

lazy val core =
  Utils.mkModule("explorer-core", "explorer-core")
    .settings(commonSettings)
    .settings(libraryDependencies ++= cats("3") ++ circe("3") ++ refined("3"))


lazy val indexer =
  Utils.mkModule("chain-indexer", "chain-indexer")
    .enablePlugins(JavaAppPackaging)
    .settings(commonSettings)
    .settings(assemblySettings("chain-indexer"))
    .settings(libraryDependencies ++= lightBend("3") ++ sttp("3") ++ cassandraDb ++ monocle("3") ++ refined("3") ++ scalatest("3") ++ logging ++ Seq(ergoWallet, pureConfig)).settings(excludeDependencies ++= cats("2.13").map( x => ExclusionRule(x.organization, x.name)) ++ circe("2.13").map( x => ExclusionRule(x.organization, x.name)))
    .dependsOn(core)
